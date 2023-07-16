import os
import datetime
import dateutil.parser

import voeventparse as vp

import astropy_healpix as ah
from astropy import units as u
import numpy as np
from io import BytesIO
from astropy.table import Table

from reading_writing import get_xml_filename, get_skymap_name
from plotter import plot_skymap
from odds_script import calculate_odds

# CONSTANTS

TIME_BEFORE_GW = datetime.timedelta(seconds=10000) 
TIME_AFTER_GW = datetime.timedelta(seconds=10000) 

#################################################

def parse_gw( notice, skymap_bytes ):
    #Preparing certain variable
    far = 1./notice['event']['far'] / (3600.0 * 24 * 365.25)
    if far>100.0:
        far = float(str(int(np.round(far))))
    else:
        far = float('%2.4f'%far)
    ext, ext_details = 'None', 'None'
    if notice['external_coinc'] != None:
        ext = '*External Detection*'
        joint_far = 1/notice['external_coinc']['time_sky_position_coincidence_far'] / (3600.0 * 24 * 365.25)
        ext_details = f"Observatory: {notice['external_coinc']['observatory']},\n\t\t"\
            f"time_difference: {notice['external_coinc']['time_difference']} seconds,\n\t\t"\
            f"search:  {notice['external_coinc']['search']},\n\t\t"\
            f"joint FAR: 1 per {joint_far} years"

    superevent_id = notice["superevent_id"]
    gracedb = f"https://example.org/superevents/{superevent_id}/view"
    img_link1 = f"https://gracedb.ligo.org/apiweb/superevents/{superevent_id}/files/bayestar.png"
    img_link2 = f"https://gracedb.ligo.org/api/superevents/{superevent_id}/files/bayestar.volume.png"
    img_link3 = f"https://gracedb.ligo.org/api/superevents/{superevent_id}/files/bayestar.fits.gz"
    img_link4 = f"http://treasuremap.space/alerts?graceids={superevent_id}"

    skymap = Table.read(BytesIO(skymap_bytes))

    level, ipix = ah.uniq_to_level_ipix(
            skymap[np.argmax(skymap['PROBDENSITY'])]['UNIQ'])
    ra, dec = ah.healpix_to_lonlat(ipix, ah.level_to_nside(level),
                                   order='nested')
    #Preparing message for slack #:7.2f
    message_text = f"Gravitational Wave Data: \n\n\
        *Superevent ID: {notice['superevent_id']}* \n\
        Event Time: {notice['event']['time']} \n\
        Notice Time: {notice['time_created']} \n\
        Alert Type: {notice['alert_type']}\n\
        Group: {notice['event']['group']} \n\
        FAR: 1 per {far} years \n\
        log BCI: {skymap.meta['LOGBCI']:7.2f} \n\
        90% Area: *{gw_area_within( skymap_bytes, 0.9):.2f}* deg^2\n\
        50% Area: *{gw_area_within( skymap_bytes, 0.5):.2f}* deg^2\n\
        Significant detection? *{notice['event']['significant']}* \n\
        Classification Probabilities: {notice['event']['classification']}\n\
        BNS % : {notice['event']['classification']['BNS']}\n\
        NSBH % : {notice['event']['classification']['NSBH']}\n\
        Most Likely Classification: {max(notice['event']['classification'], key=notice['event']['classification'].get)}\n\
        Has_NS: *{notice['event']['properties']['HasNS']}* \n\
        Has_Remnant: *{notice['event']['properties']['HasRemnant']}* \n\
        Has_Mass_Gap: {notice['event']['properties']['HasMassGap']}\n\
        Distance (Mpc): *{skymap.meta['DISTMEAN']:7.2f} with error {skymap.meta['DISTSTD']:7.2f}* \n\
        RA, DEC: {ra.deg}, {dec.deg} \n\
        Detection pipeline: {notice['event']['pipeline']}\n\
        Detection instruments: {notice['event']['instruments']}\n\
        Any external detection: {ext}\n\
        External Detection Details: {ext_details} \n\
        Join Related Channel: #{notice['superevent_id'].lower()} \n\
        Skymap Image: {img_link1} \n\
        Bayestar Volume Image: {img_link2} \n\
        Bayestar Skymap Download Link (Click to download): {img_link3} \n\
        Treasure Map Link: {img_link4} \n\
        "  
    return message_text

def gw_area_within( skymap_bytes:bytes, prob:float ):
    assert prob < 1, "Enter `prob` parameter in decimal format"
    skymap = Table.read(BytesIO(skymap_bytes))
    # Sort the pixels of the sky map by descending probability density
    skymap.sort('PROBDENSITY', reverse=True)
    
    # Find the area of each pixel
    level, ipix = ah.uniq_to_level_ipix(skymap['UNIQ'])
    pixel_area = ah.nside_to_pixel_area(ah.level_to_nside(level))
    
    # Calculate the probability within each pixel: the pixel area times the
    #      probability density
    prob_density = pixel_area * skymap['PROBDENSITY']
    
    # Calculate the cumulative sum of the probability
    cumprob = np.cumsum(prob_density)

    # Find the pixel for which the probability sums to 0.9 (90%)
    i = cumprob.searchsorted(prob)

    # The area of the 90% credible region is simply the sum of the areas of
    #      the pixels up to that one
    area_list = pixel_area[:i].sum()

    return area_list.to_value(u.deg**2)

#################################################

def parse_frb( voevent ):
    groups = vp.get_grouped_params(voevent)
    known_source = groups['event parameters']['known_source_name']['value'] if groups['event parameters']['known_source_name']['value'] != '' else "None"

    message_text = f"FRB Data:\n\n\
        IVORN: {voevent.attrib['ivorn']}\n\
        Event No: {groups['event parameters']['event_no']['value']}\n\
        Known Source: {known_source}\n\
        Above Source Association: {voevent.Why.Inference.attrib['probability']}\n\
        Event Time (@400 MHz,correction for dispersion):{groups['event parameters']['timestamp_utc']['value']}\n\
        Notice Time: {voevent.Who.Date}\n\
        Dispersion Measure: {groups['event parameters']['dm']['value']} {groups['event parameters']['event_no']['unit']}\n\
        Event Type: {groups['event parameters']['event_type']['value']}\n\
        Pipeline name: {groups['event parameters']['pipeline_name']['value']}\n\
        SNR: {groups['event parameters']['snr']['value']}\n\
        Importance: {voevent.Why.attrib['importance']}"
    
    return message_text

#################################################

def parse_message(gw_data, skymap_bytes, frb_data, odds):
    return f"*Possible Associated Event*\n\
    Odds of Common Source:\
        minimum FRB z: {odds[0]:.2E}\n\
        maximum FRB z: {odds[1]:.2E}\n\n\n\
    {parse_gw(gw_data, skymap_bytes)}\n\n\
    {parse_frb(frb_data)}"

#################################################

def frb_location( voevent ):
    astro_coords = voevent.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords
    ra = astro_coords.Position2D.Value2.C1
    dec = astro_coords.Position2D.Value2.C2
    error_radius = astro_coords.Position2D.Error2Radius
    return float(ra), float(dec), float(error_radius)

def gw_prob_list( skymap_bytes:bytes, frb_index:int, prob:float,):
    assert prob < 1, "Enter `prob` parameter in decimal format"
    skymap = Table.read(BytesIO(skymap_bytes))
    
    # Sort the pixels of the sky map by descending probability density
    sorter = skymap.argsort('PROBDENSITY', reverse=True)

    # Determine location of frb_index
    frb_new_index = np.where(sorter==frb_index)[0][0]

    # Find the area of each pixel
    level, ipix = ah.uniq_to_level_ipix(skymap['UNIQ'])
    pixel_area = ah.nside_to_pixel_area(ah.level_to_nside(level))
    
    # Calculate the probability within each pixel: the pixel area times the
    #      probability density
    prob_density = pixel_area * skymap['PROBDENSITY']
    
    # Calculate the cumulative sum of the probability
    cumprob = np.cumsum(prob_density[sorter])

    # Find the pixel for which the probability sums to 0.9 (90%)
    cut_off = cumprob.searchsorted(prob)

    return cut_off, frb_new_index

def gw_search( ra:float, dec:float, skymap_bytes:bytes):
    skymap = Table.read(BytesIO(skymap_bytes))
    # Finds pixel by sky location
    # https://emfollow.docs.ligo.org/userguide/tutorial/multiorder_skymaps.html
    ra *= u.deg
    dec *= u.deg

    # First, find the NESTED pixel index of every multi-resolution tile, at an
    #      arbitrarily high resolution
    max_level = 29
    max_nside = ah.level_to_nside(max_level)
    level, ipix = ah.uniq_to_level_ipix(skymap['UNIQ'])
    index = ipix * (2**(max_level - level))**2
    # Sort the pixels by this value
    sorter = np.argsort(index)
    # Determine the NESTED pixel index of the target sky location at that resolution
    match_ipix = ah.lonlat_to_healpix(ra, dec, max_nside, order='nested')

    # Do a binary search for that value
    i = sorter[np.searchsorted(index, match_ipix, side='right', sorter=sorter) - 1]

    return i #skymap[i]['PROBDENSITY'].to_value(u.deg**-2)

def frb_within_90( voevent, skymap_bytes, logger ):
    # This requires testing!! (Michael's code)
    frb_ra, frb_dec, frb_uncertainty = frb_location( voevent )

    # Get pixel index of location
    frb_index = gw_search( frb_ra, frb_dec, skymap_bytes )
    # Get 90% list
    cutoff, frb_sorted_index = gw_prob_list( skymap_bytes, frb_index, 0.9 )
    #logger.info(f"cutoff: {cutoff}")
    #logger.info(f"FRB index: {frb_sorted_index}")
    if( cutoff > frb_sorted_index ):
        logger.info("The FRB is within the 90% probability region of the GW")
        return frb_sorted_index
    else:
        logger.info("The FRB is NOT within the 90% probability region of the GW")
        return -1


def determine_relation( gw_data, frb_data, slackbot, logger ):
    logger.info(f"Determining relation: {gw_data['superevent_id']}.avro & {get_xml_filename(frb_data.attrib['ivorn'], logger)}.xml")

    # Making datetime objects for easy comparison
    gw_time = dateutil.parser.isoparse(gw_data['event']['time'])
    frb_time = dateutil.parser.parse(str(frb_data.Who.Date))

    if ((gw_time - frb_time) < TIME_BEFORE_GW) and ((frb_time - gw_time) < TIME_AFTER_GW):
        logger.info("The events are within the defined plausible time region")
        logger.info(f"\tGW:  {gw_time}")
        logger.info(f"\tFRB: {frb_time}")
        if 'skymap' in gw_data['event'].keys():
            #logger.info("has skymap")
            skymap_bytes = gw_data.get('event', {}).pop('skymap')
            frb_pixel = frb_within_90( frb_data, skymap_bytes, logger )
            if frb_pixel != -1:
                frb_ra, frb_dec, frb_error = frb_location( frb_data )
                dm = vp.get_grouped_params(frb_data)['event parameters']['dm']['value']
                odds = calculate_odds(skymap_bytes, frb_ra, frb_dec, frb_error, frb_pixel, float(dm), np.abs((TIME_BEFORE_GW + TIME_AFTER_GW)/ datetime.timedelta(days=1)))
                message = parse_message(gw_data, skymap_bytes, frb_data, odds)
                image_filename = plot_skymap( get_skymap_name( gw_data, logger ) ,frb_ra, frb_dec)
                
                slackbot.post_message( title="GW-FRB Coincidence Found", message_text=message)
                slackbot.post_skymap(image_filename, frb_data.attrib['ivorn'])

                os.remove(image_filename)
                return True
        else:
            logger.info("Does not have skymap")
    else:
        logger.info("The events are NOT within the defined plausible time region")
        # Deleting file outside of the time range (one is guaranteed to be within
        #    as it just triggered)
        filename = None
        if ( gw_time < frb_time ):
            # GW came first (is too old)
            filename = os.path.join("GW_Avros", gw_data['superevent_id']+".avro")
            os.remove(filename)
        else:
            pass
            # FRB came first (is too old)
            filename = os.path.join("FRB_XMLs", get_xml_filename(frb_data.attrib["ivorn"],logger)+".xml")
            os.remove(filename) 
        logger.info(f"Removed {filename}")
    return False

