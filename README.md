# Thesis


## Import files:
Thesis/src/org/processmining/plugins/log/
Json simple: OpenNaiveLogFilePluginJsonSimple.java
Jackson normal: OpenNaiveLogFilePluginJackson.java
Jackson iterator: OpenNaiveLogFilePluginJackson_iter.java
Jsoniter normal: OpenNaiveLogFilePluginJsoninter.java
Jsoninter iterator:  OpenNaiveLogFilePluginJsoninter_iter.java
Gson normal: OpenNaiveLogFilePluginGson.java



## Export files:
Thesis/src/org/processmining/plugins/log/exporting/
The main code is in the serializer files

There are two types of GSON export
JxesGsonSerializer_iter.java uses the GSON writer iterator
JxesGsonSerializer.java uses normal GSON objects.

There are two types of Jackson export:
JxesJacksonSerializer_iter.java uses the Jackson generator iterator
JxesJacksonSerializer.java uses normal Jackson objects.

