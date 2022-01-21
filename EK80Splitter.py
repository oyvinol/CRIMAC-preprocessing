from io import BufferedWriter
import logging
import os
import sys
import struct as struct
from collections import namedtuple
from xml.dom.minidom import parseString
    
# Return full datagram as tuple (head,data) or None
def get_dg(inp_fp):
    data = ek_read_dg (inp_fp)
    if not data or len(data) == 0:
        return None
    #current_dg = ekDatagram (data)

    current_dg = data
    # Always the next datagram is rawdata payload.  We make up for a
    # difficult API by hiding it here as part of the params datagram.
    # if current_dg.dg_type == codes['rdl_raw']:
    #     current_dg.rawdata = em_read_dg(inp_fp)

    # Payload is now preserved *inside* the DG
    return (current_dg)

# Used to iterate over datagrams for convenience 
# Could speed up by using em_read_head instead of get_dg
def get_dgs_generator(inp_fp, match_types = None):
    if not inp_fp:
        return 
    
    while True:
        try:
            dg = get_dg(inp_fp)
        except Exception as e:
            logging.exception(e)
            dg = None

        if dg:
            if match_types == None:
                yield dg
            else:
                if dg.dg_type in match_types:
                    yield dg
        else:
            break

def ek_read_head(stream, noskip_types=[], force_full_read=False):
    buff = stream.read(struct.calcsize(ekDatagram.headDesc))
    if len(buff) == 0:
        return b''

    Head = ekDatagram.head._make(struct.unpack_from(ekDatagram.headDesc, buff))
    #print(Head);
    remaining_bytes = Head.DgLength - struct.calcsize(ekDatagram.headDesc)
    data = b''
    if not force_full_read and (Head.DgType not in noskip_types) and stream.seekable():
        stream.seek(remaining_bytes, 1)
    else:
        data = stream.read(remaining_bytes + 8)
        if len(data) != remaining_bytes + 8:
            print('oh no not enough data to fulfil the request, early EOF? expected to be able to read %d, actually read %d' % (remaining_bytes, len(data)))
    fulldata = buff + data
    return (Head, fulldata)

def ek_read_dg(stream, skip=False):
    force_full_read = not skip
    data = ek_read_head(stream, force_full_read = force_full_read)
    if data and data[1]:
        return data
    else:
        return None

def ek_write_dg(stream, dg, update_checksum = 1):
    return stream.write(dg.Data)

def extract_channels(dg):
    channels = []
    dgStr = str(dg)
    #print(dgStr)
    dgStr = dgStr.replace('\n', ' ').replace('\r', '')
    channelsStart = dgStr.find("<Channels>")
    channelsEnd = dgStr.find( "</Channels>")
    try:
        dgStr = dgStr[channelsStart:channelsEnd+11]
        xmldoc = parseString(dgStr)
        itemlist = xmldoc.getElementsByTagName('Channel')
        print("found " + str(len(itemlist)) + " channels")
        for channel in itemlist:
            channels.append(channel.attributes['ChannelID'].value)
            transducers = channel.getElementsByTagName('Transducer')
            
    except Exception as e:
        print("could not parse configuration for channels, inspect channels.xml for clues")
        print(e)
        config = open("channels.xml", "w")
        config.write(dgStr);
        config.close()
    return channels

def extract_channel(dg):
    channel = ''
    dgStr = str(dg)
    #print(dgStr)
    dgStr = dgStr.replace('\n', ' ').replace('\r', '')
    channelsStart = dgStr.find("<Parameter>")
    channelsEnd = dgStr.find( "</Parameter>")
    try:
        dgStr = dgStr[channelsStart:channelsEnd+12]
        xmldoc = parseString(dgStr)
        itemlist = xmldoc.getElementsByTagName('Channel')
        #print("found " + str(len(itemlist)) + " channels")
        for channel in itemlist:
            #print(channel.attributes['ChannelID'].value + " with frequency ", end='')
            channel = str(channel.attributes['ChannelID'].value)
            
    except Exception as e:
        print("could not parse configuration for parameter, inspect parameter.xml for clues")
        print(e)
        config = open("parameter.xml", "w")
        print(dgStr)
        print(str(dg))
        config.write(dgStr);
        config.close()
        exit()
    return channel

def extract_separator(dg):
    frequency = 0
    dgStr = str(dg);
    #print(dgStr)
    mode = 'CW'
    
    channelStart = dgStr.find("<Channel")
    channelEnd = dgStr.find( "/>", channelStart)
    if "<Configuration>" in dgStr:
        frequency = -1
        mode = 'Config'
    elif "<InitialParameter>" in dgStr:
        frequency = -1
        mode = 'Initial'
    elif "<Environment" in dgStr:
        frequency = -1
        mode = 'Environment'
    elif "<Sensor" in dgStr:
        frequency = -1
        mode = 'Sensor'    
    elif "<Channel" in dgStr:
        try:
            startOfChannelXml = channelStart
            endOfChannelXmml = channelEnd+2
            dgStr = dgStr[startOfChannelXml:endOfChannelXmml]
            xmldoc = parseString(dgStr)
            itemlist = xmldoc.getElementsByTagName('Channel')
            foundChannel = len(itemlist)
            if(foundChannel > 0) :
                pulseform = itemlist[0].attributes['PulseForm'].value
                if(pulseform == '0') :
                    #CW data has the tag "["
                    frequency = itemlist[0].attributes['Frequency'].value
                if(pulseform == '1') :
                    frequency = itemlist[0].attributes['FrequencyStart'].value
                    mode = 'FM'
        except:
            print(str(dg))
            print(str(dg).find("<Channel"))
            print(str(dg).find("/>", str(dg).find("<Channel")))
    elif "Ping" in dgStr:
        frequency = -1
        mode = 'Ping'
    else:
        print(dgStr)
    
    return (frequency, mode)

class ekDatagram:
    headDesc = 'i4s'
    bodyDesc = 'iil'

    head = namedtuple('EkHead', 'DgLength DgType')
    body = namedtuple('EkBody', '')
    
    dg_type = ord('?')

    # TODO cache these?
    def _get_length(self):
        return struct.calcsize(self.headDesc) + len(self.Data)

    def _gen_buf(self, update_checksum = 0):
        length = self._get_length()
        buff = bytearray(length)

        body_start = struct.calcsize(self.headDesc)
        body_end   = body_start + struct.calcsize(self.bodyDesc)
        
        # Fix size to be bytes following length field
        aa = self.Head
        bb = self.Body

        if (length != self.Head.Length):
            raise(Exception('Datagram header length invalid, cant write'))

        aa = self.Head._replace(Length = self.Head.Length - self.lenskip)

        struct.pack_into(self.headDesc, buff, 0         , *aa)
        struct.pack_into(self.bodyDesc, buff, body_start, *bb)

        # Nasty hack but hey it should work
        struct.pack_into('%ds' % len(self.Data), buff, body_end, self.Data)

        return buff
    
    def __init__ (self, buff=None):
        # If given datastream we unpack to valid datagram
        if buff:
            self._buff = buff[:] # Copy buffer to here
            self.Head = self.head._make(struct.unpack_from(self.headDesc, buff))
    
            data_meas = self.Head.DgLength \
                        - struct.calcsize(self.bodyDesc) \
                        - struct.calcsize(self.headDesc)
            self.Data = self.Head.Data
        
            if len(self.Data) != data_meas and len(self.Data) != (data_meas + 1):
                logging.error('Datagram internal sizes non-matching... (data actual %d, data expected %d)' % (len(self.Data), data_meas))

            # Default to ? for unknown DGs.
            self.dg_type = getattr(self.Head, 'Type', ord('?'))
        else:
            print("Called in a depricated way.")

size = 1000000
sizeMultiplier = 1000000

splitOnChannel = False
splitOnSize = False
splitOnMode = False


if(len(sys.argv) > 2):
    splittype = sys.argv[2]
    if(splittype == '-h'):
        print("Usage EK80Splitter.py [raw file] [mode] [param]")
        print("eg py EK80Splitter.py input.raw size 100")
        print("eg py EK80Splitter.py input.raw mode")
        print("available split modes is size [inputsize in MB], mode or channel")
        exit()
    if(splittype == "channel"):
        splitOnChannel = True
    elif(splittype == "size"):
        splitOnSize = True
        if(len(sys.argv) > 3):
            size = int(sys.argv[3]) * sizeMultiplier
            print("split size " + str(size))
    elif(splittype == "mode"):
            splitOnMode = True     
else:
    print("Usage EK80Splitter.py [raw file] [mode] [param]")
    print("eg py EK80Splitter.py input.raw size 100")
    print("eg py EK80Splitter.py input.raw mode")
    print("available split modes is size [inputsize in MB], mode or channel")
    exit()   
    
#TODO, create argument library or split to separate file.
filename = sys.argv[1];
baseName = os.path.splitext(filename)[0]
print('Splitting ' + str(filename))
            
if(splitOnChannel):
    print("Splitting on channel");

if(splitOnSize):
    print("Splitting on size")
    
if(splitOnMode):
    print("Splitting on mode")


with open(filename, 'rb') as dg_file:
        filecounter = 0
        position = 0
        offset = 0
        outnum = 0
        CWfrequencies = {}
        FMfrequencies = {}
        xmlcounter = 0
        isrdldata = 0
        configuration = bytearray() 
        initialparameter = bytearray() 
        environment = bytearray()  
        outputfileCW = BufferedWriter
        outputfileFM = BufferedWriter
        outputfile = BufferedWriter
        currentChannelFiles = []
        currentChannelFile = BufferedWriter
        
        
        if(splitOnSize):
            outputfile = open(baseName + '_size' + str(filecounter) + '.raw', 'wb')
        if(splitOnMode):
            outputfileCW = open(baseName + '_CW.raw', 'wb')
            outputfileFM = open(baseName + '_FM.raw', 'wb')
            currentMode = 'CW'
            
        for dg in get_dgs_generator(dg_file):
            position = position + len(dg[1])
            if(splitOnSize):
                outputfile.write(dg[1]);
            outnum = outnum + 1
            dgType = dg[0].DgType
            
            separator = {0,'Init'}
            frequency = 0
            mode = 'Init'
            
            if(dgType == b'XML0'):
                xmlcounter = xmlcounter + 1
                separator = extract_separator(dg[1])
                frequency = separator[0]
                mode = separator[1]
                currentMode = mode
                if(mode == 'CW'):
                    if frequency not in CWfrequencies:
                        CWfrequencies[frequency] = 0
                    CWfrequencies[frequency] = CWfrequencies[frequency] +1
                if(mode == 'Environment'):
                    print("Enviroment captured")
                    environment = dg[1]
                if(mode == 'Initial'):
                    print("Initial Parameter captured")
                    initialparameter = dg[1]
                if(mode == 'Config'):
                    print("Configuration captured")
                    configuration = dg[1]
                    if(splitOnChannel):
                        #create filehandles for channels
                        print("identifying channels")
                        channels = extract_channels(dg)
                        print("found " + str(len(channels)) + " channels")
                        for channel in channels:
                            channelName = channel.replace('|','_')
                            print("creating channel " + channelName)
                            filehandle = open(baseName + channelName + '.raw', 'wb')
                            currentChannelFiles.append(filehandle)
                        
                    
            #SPLIT on MODE
            if(splitOnMode):
                if(dgType == b'XML0'):            
                    if(currentMode == 'CW'):
                        outputfileCW.write(dg[1])
                    elif(currentMode == 'FM'):
                        outputfileFM.write(dg[1])
                    else:
                        outputfileCW.write(dg[1])
                        outputfileFM.write(dg[1])
                elif(dgType == b'RAW3'):
                    if(currentMode == 'CW'):
                        outputfileCW.write(dg[1])
                    else:
                        outputfileFM.write(dg[1])
                elif(dgType == b'RAW4'):
                    if(currentMode == 'CW'):
                        outputfileCW.write(dg[1])
                    else:
                        outputfileFM.write(dg[1])
                else:
                    outputfileCW.write(dg[1])
                    outputfileFM.write(dg[1])
                    
            #SPLIT on SIZE  
            if(splitOnSize):      
                if(dgType == b'RAW3'):
                    #we allways want to split after a raw3 to keep the xml0 and raw3 together
                    if(position > (size + filecounter*size)):
                        print("splitting file due to size")
                        outputfile.flush()
                        outputfile.close()
                        filecounter = filecounter + 1
                        outputfile = open(baseName + '_size' + str(filecounter) + '.raw', 'wb')
                        outputfile.write(configuration)
                        outputfile.write(initialparameter)
                        outputfile.write(environment)
                        
            #SPLIT on Channel
            if(splitOnChannel):
                if(dgType == b'XML0'):
                    separator = extract_separator(dg[1])
                                            
                    if(int(separator[0]) > 0):
                        #it is a ping
                        currentChannel = extract_channel(dg[1])
                        currentChannel = currentChannel.replace('|','_')
                        #set active file handle
                        for filehandle in currentChannelFiles:
                            #print(filehandle)
                            position = filehandle.name.find(currentChannel)                           
                            if(position > 0):                                
                                #set current file to write to, figure out the "if"
                                currentChannelFile = filehandle                                
                        currentChannelFile.write(dg[1])
                    else:
                        #NOT a PING, write to all files
                        for filehandle in currentChannelFiles:
                            filehandle.write(dg[1]) 
                elif(dgType == b'RAW3'):
                    currentChannelFile.write(dg[1])
                elif(dgType == b'RAW4'):
                    currentChannelFile.write(dg[1])
                else:
                    for filehandle in currentChannelFiles:
                        filehandle.write(dg[1]) 
                    
        
        if(splitOnMode):
            outputfileCW.flush()
            outputfileFM.flush()
            outputfileFM.close()
            outputfileCW.close()
        
        if(splitOnSize): 
            outputfile.flush()
            outputfile.close()
            
        if(splitOnChannel):
            for filehandle in currentChannelFiles:
                filehandle.flush()
                filehandle.close()
        
        #END Summary
        for freq in CWfrequencies:
            print('Frequency ', end='')
            print(freq, end='')
            print(" has ", end='')
            print(CWfrequencies[freq] , end='')
            print(" pings ")

        logging.debug('Closing files')
        
#TOOD handle file object close more spesifically?

