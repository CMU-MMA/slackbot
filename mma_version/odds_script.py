from astropy_healpix import HEALPix, pixel_resolution_to_nside
from astropy import units as u
from astropy.coordinates import ICRS, SkyCoord

from astropy.table import Table
from io import BytesIO

from scipy import stats

from astropy.cosmology import FlatLambdaCDM
from bilby.gw.conversion import redshift_to_luminosity_distance
import pygedm

from ligo.raven.search import skymap_overlap_integral

import numpy as np

#Hubble Constant
H0 = 70

def dL_from_DM(frb_dm, frb_ra, frb_dec):
    """Determine the luminosity distance of an FRB from its sky location
    and dispersion measure
    
    Parameters
    ---------
    `frb_dm` : float
        dispersion measure, pc/cm^3
    `frb_ra` : float
        ra in deg
    `frb_dec` : float
        dec in deg
        
    Returns
    ------
    `dL` : float
        calculated luminosity distance
    """


    #bns nsbs, also run against catalogue of repeating frb list

    # This function is from Mohit Bhardwaj
    # Get z from DM
    c = SkyCoord(ra=frb_ra*u.degree, dec=frb_dec*u.degree)
    l,b = c.galactic.l.value, c.galactic.b.value
    distance = 30000 # in pc
    DM1, tau_sc = pygedm.dist_to_dm(l,b, distance, method='ne2001')
    DM2, tau_sc = pygedm.dist_to_dm(204.0, -6.5, distance, method='ymw16')
    DM_galactic = max(DM1, DM2) # maximum of the two values
    # Macquart relation — maximum redshift constraint
    z_max = (frb_dm-DM_galactic)/1000
    z_min = (frb_dm-150-DM_galactic)/1000 #host galactric


    #Baysian redshift estimate code

    # Get dL from z
    cosmo = FlatLambdaCDM(H0=H0,Om0=0.308)
    dL_min = redshift_to_luminosity_distance(z_min,cosmo)
    dL_max = redshift_to_luminosity_distance(z_max,cosmo)
    return dL_min, dL_max

def create_external_skymap(ra, dec, chime_error):
    """Create a sky map, either a gaussian or a single
    pixel sky map, given an RA, dec, and error radius.


    Parameters
    ----------
    `ra` : float
        right ascension in deg
    `dec` : float
        declination in deg
    `chime_error` : float
        <95% confidence in deg—really it is max(pos_error_semiminor_deg_95,pos_error_semimajor_deg_95)

    Returns
    -------
    `skymap` : numpy array
        sky map array

    """
    # This function is from Ignacio Hernandez
    max_nside = 2048
    
    # for chime_error ~ 95% condifence interval
    standatd_deviation = chime_error / 2
    # Correct 90% containment to 1-sigma for Swift
    error_radius = standatd_deviation * u.deg
    nside = pixel_resolution_to_nside(error_radius, round='up')

    if nside >= max_nside:
        nside = max_nside

        #  Find the one pixel the event can localized to
        hpx = HEALPix(nside, 'ring', frame=ICRS())
        skymap = np.zeros(hpx.npix)
        ind = hpx.lonlat_to_healpix(ra * u.deg, dec * u.deg)
        skymap[ind] = 1.
    else:
        #  If larger error, create gaussian sky map
        hpx = HEALPix(nside, 'ring', frame=ICRS())
        ipix = np.arange(hpx.npix)

        #  Evaluate Gaussian.
        center = SkyCoord(ra * u.deg, dec * u.deg)
        distance = hpx.healpix_to_skycoord(ipix).separation(center)
        skymap = np.exp(-0.5 * np.square(distance / error_radius).to_value(
            u.dimensionless_unscaled))
        skymap /= skymap.sum()


    # Renormalize due to possible lack of precision
    # Enforce the skymap to be non-negative
    return np.abs(skymap) / np.abs(skymap).sum()

def distance_overlap( gw_skymap, DM, frb_index, frb_ra, frb_dec ):
    '''Using Singer 2016 GOING THE DISTANCE: MAPPING HOST GALAXIES OF LIGO AND VIRGO SOURCES IN THREE DIMENSIONS
    USING LOCAL COSMOGRAPHY AND TARGETED FOLLOW-UP

    '''
    N = gw_skymap["DISTNORM"][frb_index]
    sigma = gw_skymap["DISTSIGMA"][frb_index]
    mu = gw_skymap["DISTMU"][frb_index]
    r = dL_from_DM(DM, frb_ra, frb_dec)

    # Equation (1) of above paper
    I_DL = N * stats.norm(loc=mu, scale=sigma).pdf(r) 
    return I_DL

def calculate_odds(gw_skymap_bytes:bytes, frb_ra, frb_dec, frb_error, frb_index, DM, search_span:float):
    '''Determine odds of common source for a GW and FRB, specific to
    the CHIME experiment and LVK

    Based on Hernandez's "On the association of GW190425 with its potential
    electromagnetic counterpart FRB 20190425A"

    Parameters
    ----------
    `gw_skymap_bytes` : dict
        bytes for skymap of GW event, as sent in `.avro` notice
    `frb_ra` : float
        right ascension of FRB messenger, in deg
    `frb_dec` : float
        declination of FRB messenger, in deg
    `frb_error` : float
        error in FRB localization (in this case, 95% interval)
    `frb_index` : int
        index of HEALPix location of FRB
    `search_span` : float
        range of temporal search, in days

    Returns
    -------
    `odds` : float
        odds of common source hypothesis being correct
    '''
    # odds = pi_cr * I_DL * I_omega * I_tc      <-- Equation (2)
    # odds = 1/(R_em \delta t) * I_DL * I_omega <-- Equation (6)

    R_em = 1.6 # day^-1
    del_t = search_span # day 
    I_DL_min, I_DL_max = distance_overlap(Table.read(BytesIO(gw_skymap_bytes)), DM, frb_index, frb_ra, frb_dec)
    I_omega = skymap_overlap_integral(create_external_skymap(frb_ra, frb_dec, frb_error), Table.read(BytesIO(gw_skymap_bytes)))

    return (1/(R_em * del_t) * I_DL_min * I_omega, 1/(R_em * del_t) * I_DL_max * I_omega)