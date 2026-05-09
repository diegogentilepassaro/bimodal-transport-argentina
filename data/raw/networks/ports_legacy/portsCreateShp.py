from qgis.core import *
from qgis.utils import iface
import os
import processing
from osgeo import gdal
from qgis.analysis import *
import os
import datetime
from shutil import copyfile
from PyQt5.QtCore import *
from qgis.PyQt.QtCore import *

print(datetime.datetime.now())

path = r"D:\cotebelmar\Dropbox\Documents\Economia\__Brown\Research\Train\derived\unitcostrasters\code"
#path = r"C:\Users\diegog\Desktop\Diego\Train\derived\unitcostrasters\code"

os.chdir(path)

pathInput=path+r"\..\..\..\raw_data"
pathOutput=path+r"\..\output"
pathTemp=path+r"\..\temp"


uri = "file:///"+pathTemp+"/ports_forgeoref.csv?encoding=%s&delimiter=%s&xField=%s&yField=%s&crs=%s" % ("UTF-8",",", "coordx", "coordy","epsg:4326")
layer=QgsVectorLayer(uri,"","delimitedtext")
if not layer.isValid():
    print ("Layer not loaded")
if layer.isValid():
    print ("Layer loaded")

input=layer
output=pathTemp+r'\\ports_georef_aux.shp'
parameters={'INPUT':input,'OUTPUT':output}
processing.run("native:fixgeometries", parameters)

input=output
output=pathTemp+r"\\ports_georef.shp"
parameters = {'INPUT':input,'TARGET_CRS':QgsCoordinateReferenceSystem('ESRI:54034'),
              'OPERATION':'+proj=pipeline +step +proj=unitconvert +xy_in=deg +xy_out=rad +step +proj=cea +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84',
              'OUTPUT':output}
processing.run("native:reprojectlayer", parameters)

uri = "file:///"+pathTemp+"/ports_ign.csv?encoding=%s&delimiter=%s" % ("UTF-8",",")
layer=QgsVectorLayer(uri,"","delimitedtext")
if not layer.isValid():
    print ("Layer not loaded")
if layer.isValid():
    print ("Layer loaded")

input=pathInput+r"\\navigation\\puertos_y_muelles\\shp\\puntos_de_puertos_y_muelles_BB005\\puntos_de_puertos_y_muelles_BB005.shp"
input2=layer
output=pathTemp+r'\\ports_ign_aux.shp'
parameters= {'INPUT':input,'FIELD':'gid','INPUT_2':input2,'FIELD_2':'gid','FIELDS_TO_COPY':[],'METHOD':1,'DISCARD_NONMATCHING':True,'PREFIX':'','OUTPUT':output}
processing.run("native:joinattributestable", parameters)

input=output
output=pathTemp+r"\\ports_ign.shp"
parameters = {'INPUT':input,'TARGET_CRS':QgsCoordinateReferenceSystem('ESRI:54034'),
              'OPERATION':'+proj=pipeline +step +proj=unitconvert +xy_in=deg +xy_out=rad +step +proj=cea +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84',
              'OUTPUT':output}
processing.run("native:reprojectlayer", parameters)


input=output
input2=pathTemp+r"\\ports_georef.shp"
output=pathTemp+r"\\ports.shp"
parameters={'LAYERS':[input, input2],'CRS':None,'OUTPUT':output}
processing.run("native:mergevectorlayers", parameters)

input=output
output=pathTemp + r"\\ports_id.shp"
parameters = {'INPUT':input,'FIELD_NAME':'AUTO','START':0,'GROUP_FIELDS':[],'SORT_EXPRESSION':'','SORT_ASCENDING':True,'SORT_NULLS_FIRST':False,'OUTPUT':output}
processing.run("native:addautoincrementalfield", parameters)

print(datetime.datetime.now())
os.kill(os.getpid(), 9)
