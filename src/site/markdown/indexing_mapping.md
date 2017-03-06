#Mapping

Illustration of a possible mapping from the resource sample listed [before](indexing_configuration.html) to the index structure. 

The *RealOffset* is the real position of the element in the resource, the *Offset* is the position adjusted to the Lucene requirement that all elements with the same starting position should have the same starting offset. 


|Id|RealOffset|Offset|Position|Parent|Payload|Prefix|Postfix|
|---|---|---|---|---|---|---|---|
|643|1985-40696|2154-39638|0|151||div|chapter|
|5|2049-2377|2154-2349|0|643||head||
|3|2298-2313|2154-2349|0|4||lemma|stemma|
|2|2242-2257|2154-2349|0|4||pos|N(soort,ev,basis,onz,stan)|
|4|2100-2364|2154-2349|0|5||s||
|0|2214-2238|2154-2349|0|4||t|Stemma|
|1|2214-2238|2154-2349|0|4||t_lc|stemma|
|41|5372-5734|2503-2875|1-2|57||dependency|su|
|40|5583-5711|2503-2698|1|41||dependency.dep||
|9|2646-2661|2503-2698|1|57||lemma|Stemma|
|642|2384-39666|2503-39638|1-151|643||p|firstparagraph|
|8|2588-2603|2503-2698|1|57||pos|N(eigen,ev,basis,zijd,stan)|
|57|2452-9269|2503-4087|1-8|642||s||
|6|2560-2584|2503-2698|1|57||t|Stemma|
|7|2560-2584|2503-2698|1|57||t_lc|stemma|
|44|5747-6111|2702-3444|2,5|57||dependency|predc|
|39|5455-5578|2702-2875|2|41||dependency.hd||
|42|5833-5956|2702-2875|2|44||dependency.hd||
|13|2826-2841|2702-2875|2|57||lemma|zijn|
|12|2783-2798|2702-2875|2|57||pos|WW(pv,tgw,ev)|
|10|2759-2779|2702-2875|2|57||t|is|
|11|2759-2779|2702-2875|2|57||t_lc|is|
|...|...|...|...|...|...|...|...|

The payload above, although never set in this example, is not the regular payload as known from Lucene. All additional elements like RealOffset, range or set of positions and the reference to a parent element are encoded together with this optional Mtas specific payload into a classic Lucene payload. This enables future use of for example confidence levels on annotations.


