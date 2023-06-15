import os
import datetime
import dateutil.parser

import voeventparse as vp

import astropy_healpix as ah
from astropy import units as u
import numpy as np
from io import BytesIO
from astropy.table import Table

from reading_writing import get_xml_filename

# CONSTANTS

TIME_BEFORE_GW = datetime.timedelta(seconds=10000) 
TIME_AFTER_GW = datetime.timedelta(seconds=10000) 


def frb_location( voevent ):
    astro_coords = voevent.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords
    ra = astro_coords.Position2D.Value2.C1
    dec = astro_coords.Position2D.Value2.C2
    error_radius = astro_coords.Position2D.Error2Radius
    return ra, dec, error_radius

def gw_prob_list( skymap, frb_index:int, prob:float,):
    assert prob < 1, "Enter `prob` parameter in decimal format"
    
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

def gw_search( ra:float, dec:float, skymap,):
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
    frb_index = gw_search( frb_ra, frb_dec, Table.read(BytesIO(skymap_bytes)) )
    # Get 90% list
    cutoff, frb_sorted_index = gw_prob_list( Table.read(BytesIO(skymap_bytes)), frb_index, 0.9 )
    #logger.info(f"cutoff: {cutoff}")
    #logger.info(f"FRB index: {frb_sorted_index}")
    if( cutoff > frb_sorted_index ):
        logger.info("The FRB is within the 90% probability region of the GW")
        return True
    else:
        logger.info("The FRB is NOT within the 90% probability region of the GW")
        return False



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
            if frb_within_90( frb_data, skymap_bytes, logger ):
                slackbot.post_message( "GW-FRB Coincidence Found", "This needs to be a lot more meaty...")
                return True
        else:
            logger.info("Does not have skymap")
    else:
        logger.info("The events are NOT within the defined plausible time region")
        return False #For testing so I don't loose all my files!
        # Deleting file outside of the time range (one is guaranteed to be within
        #    as it just triggered)
        filename = None
        if ( gw_time < frb_time ):
            # GW came first (is too old)
            filename = os.path.join("GW_Avros", gw_data['superevent_id']+".avro")
            os.remove(filename)
        else:
            # FRB came first (is too old)
            filename = os.path.join("FRB_XMLs", get_xml_filename(frb_data.attrib["ivorn"])+".xml")
            os.remove(filename) 
        logger.info(f"Removed {filename}")
    return False

