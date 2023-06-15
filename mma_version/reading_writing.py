# 3rd file to store reading/writing files required to deal with the GW avro
# events and the FRB XML events

import os
import string

from fastavro import writer, reader, parse_schema
import voeventparse as vp

from twisted.python import lockfile
# Using lockfile to ensure our reading/writing is uninterrupted


FRB_DIRECTORY = os.path.join(os.getcwd(),"FRB_XMLs")
GW_DIRECTORY = os.path.join(os.getcwd(),"GW_Avros")

# GENERAL FUNCTIONS #######################################

def get_file_names( GW=True ):
    directory = 'GW_Avros' if GW else "FRB_XMLs"
    #files = [os.path.join(directory, f) for f in os.listdir(directory) if
    #         os.path.isfile(os.path.join(directory, f))]
    try:
        files = [ f for f in os.listdir(directory) if
                os.path.isfile(os.path.join(directory, f))]
        return files
    except FileNotFoundError:
        return []

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
    
    lock = lockfile.FilesystemLock(file_name + ".lock")
    lock.lock()
    try:
        with open(file_name, "rb") as fo:
            avro_reader = reader(fo)
            record = next(avro_reader)
    finally:
        lock.unlock()
    return record

def write_avro_file( message, logger, alerted_slack=False ):
    # Using the same schema with an added "alerted_slack" attribute (default False)
    schema = message.schema
    schema['fields'].append({'doc': 'Record of if we sent this to Slack.',
                             'name': 'alerted_slack', 'type': 'boolean'})
    message.content[0]['alerted_slack'] = alerted_slack
    parsed_schema = parse_schema(schema)

    if not os.path.exists( GW_DIRECTORY ):
        os.makedirs( GW_DIRECTORY )
    file_name = os.path.join( GW_DIRECTORY, message.content[0]['superevent_id']+".avro")
    
    lock = lockfile.FilesystemLock(file_name + ".lock")
    lock.lock()
    try:
        logger.debug(f"Writing incoming GW notice to {file_name}...")
        with open(file_name, 'wb') as out:
            writer(out, parsed_schema, message.content)
        logger.debug("Done")
    finally:
        lock.unlock()
    return file_name


# XML FUNCTIONS ###########################################

def read_xml_file( file_name ):
    file_name = os.path.join( FRB_DIRECTORY, file_name)

    lock = lockfile.FilesystemLock(file_name + ".lock")
    lock.lock()
    try:
        with open(file_name, "rb") as f:
            # Load VOEvent XML from file
            voevent = vp.load(f)
    finally:
        lock.unlock()
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
    file_name = os.path.join( FRB_DIRECTORY, get_xml_filename(event.attrib["ivorn"], logger)+".xml" )

    lock = lockfile.FilesystemLock(file_name + ".lock")
    lock.lock()
    try:
        logger.debug(f"Writing incoming GW notice to {file_name}...")
        with open( file_name , 'wb') as f:
            vp.dump(event, f)
        logger.debug("Done")
    finally:
        lock.unlock()
    return file_name
