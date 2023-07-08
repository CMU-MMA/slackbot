import ligo.skymap.plot
from matplotlib import pyplot as plt
from matplotlib import rcParams
from ligo.skymap import io, postprocess
import astropy_healpix as ah
from astropy.coordinates import SkyCoord
from astropy import units as u
import numpy as np

def safe_save_figure(fig, filename, logger, **kwargs):
    try:
        fig.savefig(fname=filename, **kwargs)
    except RuntimeError:
        logger.debug(
            "Failed to save plot with tex labels turning off tex."
        )
        rcParams["text.usetex"] = False
        fig.savefig(fname=filename, **kwargs)


def plot_skymap( filename, ra, dec, logger ):
    if filename is None or filename == "":
        logger.error("Please give a valid filename to read from")
        return ""
    fits_filename = filename
    png_filename = fits_filename[:-5]+".png"
    print(fits_filename)
    logger.info(f"Writing image to {png_filename}")


    skymap, metadata = io.fits.read_sky_map( fits_filename, nest=None )
    nside = ah.npix_to_nside(len(skymap))

    # Convert sky map from probability to probability per square degree.
    deg2perpix = ah.nside_to_pixel_area(nside).to_value(u.deg**2)
    probperdeg2 = skymap / deg2perpix

    ax = plt.axes( projection='astro hours mollweide' )
    ax.grid()

    ax.plot_coord(
            SkyCoord(ra, dec, unit='deg'), '.',
            markerfacecolor='white', markeredgecolor='blue', markersize=10)

    vmax = probperdeg2.max()
    img = ax.imshow_hpx((probperdeg2, 'ICRS'), nested=metadata['nest'], vmin=0., vmax=vmax,
                        cmap='cylon')
    
    colorbar = True
    if colorbar:
        from ligo.skymap import plot
        cb = plot.colorbar(img)
        cb.set_label(r'prob. per deg$^2$')

    contour = [50,90]
    if contour is not None:
        cls = 100 * postprocess.find_greedy_credible_levels(skymap)
        cs = ax.contour_hpx(
            (cls, 'ICRS'), nested=metadata['nest'],
            colors='k', linewidths=0.5, levels=contour)
        fmt = r'%g\%%' if rcParams['text.usetex'] else '%g%%'
        plt.clabel(cs, fmt=fmt, fontsize=6, inline=True)

    plot.outline_text(ax)

    safe_save_figure(fig=plt.gcf(), filename=png_filename, logger=logger, dpi=100)
    
    return png_filename



        

