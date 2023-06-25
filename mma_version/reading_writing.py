# 3rd file to store reading/writing files required to deal with the GW avro
# events and the FRB XML events

import os
import string

from fastavro import writer, reader, parse_schema
import voeventparse as vp

import fasteners #https://pypi.org/project/fasteners/
from contextlib import contextmanager

FRB_DIRECTORY = os.path.join(os.getcwd(),"FRB_XMLs")
FRB_SAVED = os.path.join(FRB_DIRECTORY,"FRB_XMLs")
GW_DIRECTORY = os.path.join(os.getcwd(),"GW_Avros")
GW_SAVED = os.path.join(GW_DIRECTORY,"GW_Avros")
SKYMAPS_DIRECTORY = os.path.join(os.getcwd(),"all_skymaps")

# LOCKING CONTEXT MANAGER #################################

@contextmanager
def read_lock( file_name ):
    lock_file = f'{file_name}.lock'
    lock = fasteners.InterProcessReaderWriterLock(lock_file)
    try:
        with lock.read_lock():
            yield
    finally:
        os.remove(lock_file)
@contextmanager
def write_lock( file_name ):
    lock_file = f'{file_name}.lock'
    lock = fasteners.InterProcessReaderWriterLock(lock_file)
    try:
        with lock.write_lock():
            yield
    finally:
        os.remove(lock_file)

# GENERAL FUNCTIONS #######################################

def get_file_names( GW=True ):
    directory = 'GW_Avros' if GW else "FRB_XMLs"
    #files = [os.path.join(directory, f) for f in os.listdir(directory) if
    #         os.path.isfile(os.path.join(directory, f))]
    files = [ f for f in os.listdir(directory) if
             os.path.isfile(os.path.join(directory, f))]
    return files

def _clear_avros():
    files = get_file_names(GW=True)
    for file in files:
        os.remove(file)

def remove_avro( filename ):
    filename = os.path.join( GW_DIRECTORY, filename )
    os.remove( filename )

def _clear_xmls():
    files = get_file_names(GW=False)
    for file in files:
        os.remove(file) 

def remove_xml( filename ):
    filename = os.path.join( FRB_DIRECTORY, filename )
    os.remove( filename )

def alerted_slack( gw_filename, frb_filename ):
    # Need to update files by deleting and then rewriting them
    sent = True

    message = read_avro_file( gw_filename )
    os.remove( gw_filename )
    write_avro_file( message, alerted_slack=sent )

    voevent = read_xml_file( frb_filename )
    os.remove( frb_filename )
    write_xml_file( voevent, alerted_slack=sent )


# AVRO FUNCTIONS ##########################################

def read_avro_file( file_name ):
    file_name = os.path.join( GW_DIRECTORY, file_name)
    with read_lock(file_name):
        with open(file_name, "rb") as fo:
            avro_reader = reader(fo)
            record = next(avro_reader)
    return record

def write_avro_file( message, logger, alerted_slack=False ):
    # Using the same schema with an added "alerted_slack" attribute (default False)
    schema = message.schema
    #print(schema)
    #print(type(schema))
    if True: #scheme #not exitsts 
        schema['fields'].append({'doc': 'Record of if we sent this to Slack.',
                             'name': 'alerted_slack', 'type': 'boolean'})
    #message.get('event', {}).pop('skymap')
    #from pprint import pprint
    #pprint(message)
    message.content[0]['alerted_slack'] = alerted_slack
    parsed_schema = parse_schema(schema)

    if not os.path.exists( GW_DIRECTORY ):
        os.makedirs( GW_DIRECTORY )
    file_name = os.path.join( GW_DIRECTORY, message.content[0]['superevent_id']+".avro")
    
    with write_lock(file_name):
        logger.debug(f"Writing incoming GW notice to {file_name}...")
        with open(file_name, 'wb') as out:
            writer(out, parsed_schema, message.content)
        logger.debug("Done")

    return file_name


# XML FUNCTIONS ###########################################

def read_xml_file( file_name ):
    file_name = os.path.join( FRB_DIRECTORY, file_name)

    with read_lock(file_name):
        with open(file_name, "rb") as f:
            # Load VOEvent XML from file
            voevent = vp.load(f)
    return voevent

def get_xml_filename( input_string, logger ):
    try:
        start_ind = input_string.index("-#")+2
        return "".join(
            x
            for x in input_string[start_ind:].replace("-", "_").replace("+", "_").replace(":", "_")
            if x in string.digits + string.ascii_letters + "_."
        )
    except ValueError:
        logger.error(f"VOEvent with IVORN {input_string} has foreign form, this should not happen")
        #This works but gives an ugly result
        return "".join([c for c in input_string if c.isalpha() or c.isdigit() or c==' ']).rstrip()


def write_xml_file( event, logger, alerted_slack=False)->str:
    # This is where the `alerted_slack` data is stored: within the 
    # event.who.description: while this is rather ugly compared to 
    # updating the schema like we do with GW avros, the schema is not
    # able to be changed here, and this is simple enough
    event.Who.Description = "CHIME/FRB VOEvent Service: "\
                           f"alerted_slack ={alerted_slack}" 
    if not os.path.exists( FRB_DIRECTORY ):
        os.makedirs( FRB_DIRECTORY )
    file_name = os.path.join( FRB_DIRECTORY, get_xml_filename(event.attrib["ivorn"])+".xml")

    with write_lock(file_name):
        logger.debug(f"Writing incoming GW notice to {file_name}...")
        with open( file_name , 'wb') as f:
            vp.dump(event, f)
        logger.debug("Done")
    return file_name


###############################################################################

def save_skymap( notice ):
    if 'skymap' in notice['event'].keys():
        skymap_bytes = notice.get('event', {}).get('skymap')
        
        if not os.path.exists( SKYMAPS_DIRECTORY ):
            os.makedirs( SKYMAPS_DIRECTORY )
        file_name = os.path.join( SKYMAPS_DIRECTORY, notice['superevent_id']+".bin")
    
        with write_lock( file_name ):
            with open(file_name, 'wb') as f: 
                f.write( skymap_bytes )

# Give function to Palmese##################
from io import BytesIO 
from astropy.table import Table 

# The files are named as message['superevent_id']+".bin"
#   All skymaps are saved for events with terrastrial < 0.5
# If more data about the event is useful, I can also just save
#   the entire notice (which would include the skymap)
def read_skymap( file_name ):
    file_name = os.path.join( SKYMAPS_DIRECTORY, file_name)
    
    with open(file_name, "rb") as f:
        skymap_bytes = f.read()
    skymap = Table.read(BytesIO(skymap_bytes))
    return skymap
##############################################