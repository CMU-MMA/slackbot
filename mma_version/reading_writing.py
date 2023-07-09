# 3rd file to store reading/writing files required to deal with the GW avro
# events and the FRB XML events

import os
import string
import datetime

from fastavro import writer, reader, parse_schema
import voeventparse as vp

import fasteners #https://pypi.org/project/fasteners/
from contextlib import contextmanager

FRB_DIRECTORY = os.path.join(os.getcwd(),"FRB_XMLs")
FRB_SENT = os.path.join(FRB_DIRECTORY,"FRB_sent")
GW_DIRECTORY = os.path.join(os.getcwd(),"GW_Avros")
GW_SENT = os.path.join(GW_DIRECTORY,"GW_sent")
SKYMAPS_DIRECTORY = '/hildafs/projects/phy220048p/share/skymaps_bot_o4'

AVRO_SCHEMA = None

EPOCH = datetime.datetime( 1970, 1, 1, 0, 0, 0 )
MAX_SAVE = datetime.timedelta( days=2 )

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
    directory = GW_DIRECTORY if GW else FRB_DIRECTORY

    try:
        files = [ f for f in os.listdir(directory) if
                os.path.isfile(os.path.join(directory, f))]
        return files
    except FileNotFoundError:
        return []
    
def get_sent_files( GW=True ):
    directory = GW_SENT if GW else FRB_SENT

    try:
        files = [ os.path.join(directory,f) for f in os.listdir(directory) if
                os.path.isfile(os.path.join(directory, f))]
        return files
    except FileNotFoundError:
        return []

def _clear_avros():
    files = get_file_names(GW=True)
    for file in files:
        os.remove(os.path.join(GW_DIRECTORY,file))

def remove_avro( filename ):
    # first running a regular check to remove any old events that
    #   have no chance of being redacted anymore
    files = get_sent_files( GW=True )
    for file in files:
        creation_time = EPOCH + datetime.timedelta(seconds=os.path.getctime(file))
        if (datetime.datetime.utcnow()-creation_time) > MAX_SAVE:
            os.remove(file)

    # actually removing passed avro
    filename = os.path.join( GW_DIRECTORY, filename )
    os.remove( filename )

def remove_fake_avro( filename ):
    filename = os.path.join( GW_SENT, filename )
    os.remove(filename)

def _clear_xmls():
    files = get_file_names(GW=False)
    for file in files:
        os.remove(os.path.join(FRB_DIRECTORY,file) )

def remove_xml( filename ):
    # first running a regular check to remove any old events that
    #   have no chance of being redacted anymore
    files = get_sent_files( GW=False )
    for file in files:
        creation_time = EPOCH + datetime.timedelta(seconds=os.path.getctime(file))
        if (datetime.datetime.utcnow()-creation_time) > MAX_SAVE:
            os.remove(file)
    
    # actually removing passed xml
    filename = os.path.join( FRB_DIRECTORY, filename )
    os.remove( filename )

def remove_fake_xml( filename ):
    filename = os.path.join( FRB_SENT, filename )
    os.remove(filename)

def alerted_slack( gw_filename, frb_filename, logger ):
    # Need to update files by deleting and then rewriting them
    sent = True

    if not os.path.exists( GW_SENT ):
        os.makedirs( GW_SENT )
    if not os.path.exists( FRB_SENT ):
        os.makedirs( FRB_SENT )

    message = read_avro_file( gw_filename )
    os.remove( os.path.join( GW_DIRECTORY,gw_filename) )
    write_avro_file( message, logger, alerted_slack=sent )
    # make empty file (name and time of create all that matter)
    open(os.path.join(GW_SENT,gw_filename),'w').close()

    voevent = read_xml_file( frb_filename )
    os.remove( os.path.join( FRB_DIRECTORY,frb_filename) )
    write_xml_file( voevent, logger, alerted_slack=sent )
    # make empty file (name and time of create all that matter)
    open(os.path.join(FRB_SENT,frb_filename),'w').close()



# AVRO FUNCTIONS ##########################################

def read_avro_file( file_name ):
    file_name = os.path.join( GW_DIRECTORY, file_name )
    with read_lock(file_name):
        with open(file_name, "rb") as fo:
            avro_reader = reader(fo)
            record = next(avro_reader)
    return record

def write_avro_file( message, logger, alerted_slack=False ):
    # Using the same schema with an added "alerted_slack" attribute (default False)
    if type(message) == dict:
        # when writing over data from an avro that we read, we don't read the
        #   schema so we have to have is saved 
        global AVRO_SCHEMA
        if AVRO_SCHEMA is not None:
            schema = AVRO_SCHEMA
            message['alerted_slack'] = alerted_slack
            parsed_schema = parse_schema(schema)

            if not os.path.exists( GW_DIRECTORY ):
                os.makedirs( GW_DIRECTORY )
            file_name = os.path.join( GW_DIRECTORY, message['superevent_id']+".avro")
            
            with write_lock(file_name):
                logger.debug(f"Writing incoming GW notice to {file_name}...")
                with open(file_name, 'wb') as out:
                    writer(out, parsed_schema, [message])
                logger.debug("Done")
        else:
            return # this only could happen if we stopped listener and then restarted using saved avros
    else:
        schema = message.schema
        schema['fields'].append({'doc': 'Record of if we sent this to Slack.',
                             'name': 'alerted_slack', 'type': 'boolean'})
        AVRO_SCHEMA = schema
    
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
        return "".join([c for c in input_string if c.isalpha() or c.isdigit()]).rstrip()


def write_xml_file( event, logger, alerted_slack=False)->str:
    # This is where the `alerted_slack` data is stored: within the 
    # event.who.description: while this is rather ugly compared to 
    # updating the schema like we do with GW avros, the schema is not
    # able to be changed here, and this is simple enough
    event.Who.Description = "CHIME/FRB VOEvent Service: "\
                           f"alerted_slack ={alerted_slack}" 
    if not os.path.exists( FRB_DIRECTORY ):
        os.makedirs( FRB_DIRECTORY )
    file_name = os.path.join( FRB_DIRECTORY, get_xml_filename(event.attrib["ivorn"], logger)+".xml")

    with write_lock(file_name):
        logger.debug(f"Writing incoming GW notice to {file_name}...")
        with open( file_name , 'wb') as f:
            vp.dump(event, f)
        logger.debug("Done")
    return file_name


###############################################################################

from io import BytesIO 
from astropy.table import Table 

def save_skymap( notice ):
    if 'skymap' in notice['event'].keys():
        skymap_bytes = notice.get('event', {}).get('skymap')
        skymap = Table.read(BytesIO(skymap_bytes))

        if not os.path.exists( SKYMAPS_DIRECTORY ):
            os.makedirs( SKYMAPS_DIRECTORY )
        file_name = os.path.join( SKYMAPS_DIRECTORY, notice['superevent_id']+".fits")
    
        with write_lock( file_name ):
            skymap.write(file_name, overwrite=True)

def get_skymap_name( notice, logger ):
    file_name = os.path.join( SKYMAPS_DIRECTORY, notice['superevent_id']+".fits")
    if not os.path.exists( file_name ):
            logger.error( f"{file_name} does not exist but should")
            return ""
    return file_name
