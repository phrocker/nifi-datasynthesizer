# nifi-accumulo

This is a basic NiFi->Accumulo integration. Running `mvn install` will create your NAR, which can be added
to Apache NiFi. This is intended to be created with Apache NiFi 1.9+, though it may work with earlier versions.

The resulting NAR will be named 'nifi-accumulo-nar'


Note that some of this code was modeled after the HBase work.

Will add a ScanAccumulo feature along with delete and updates within PutRecord.