# LightBox Fuzzy Matching 

Note: all data has been removed from repo
* [Colorado County Parcel Data](https://gisftp.colorado.gov/)
* Lightbox data purchased from [LightBox](https://www.lightboxre.com/)

## Process
* df_split.py
  * splits full dataset into each individual county
* geometry.py
  * normalizes columns between all parcel datasets and isolates the parcel id field
  * merges multipart parcels to create single geometry
  * create id for each _unique_ geometry and merge back to file
  * outputs unique geometry file containing only the geometry and the geometry id
  * outputs full text file containing parcel id, address information for each address, and geometry id (used for merge)
* merge.py
  * iteratively merges lb parcel data to geometry with increasingly flexible logic
  * description below
* final_join.py
  * removes duplicated parcel entries due to geometry
    * outputs geometry modification file
    * duplicated parcels were created through parcels containing the exact same address. These were then manually identified to be either easements or abutting parcels on the same property
    * Note: this is valid for this particular project but should be looked at for any other use. This only effected Denver County parcels
  * joins individual files into master regional file with geometry id

## Merge Logic
Each step removes matching parcels and only sends the non matching parcels to the next step
* Step I: Spatial Files
  * Denver and Douglas both had point attributes - these were spatially joined to the unique parcel ids (to exclude one-to-many joins that would have duplicated the data)
  * Because most files did not have the spatial attribute they were started on Step II
* Step II: Parcel ID Match
  * direct join of parcel_apn to parcel_id
* Step III: Situs Match
  * Full addresses were created and cleaned either from the file or created using the individual address parts. These were set to match formatting and direct joined
* Step IV: Fuzzy Match
  * Each address from the parcel files was split into 1) street number and 2) Remainder of street info (street name with prefix and suffixes, and unit number). The remainer from the LB parcels was then fuzzy matched to the county addresses using partial ratio scoring to select the 5 most likely matches as match ids. Then for each of the 5 matches, the LB parcels were joined to the county parcel addresses using the match ids and the street number. Isolating the street number ensured that the match was accurate and prevented "3500 N Lowell St" from being matched to "2500 N Lowell Street". By using the top 5 and iterating through based on match score, this accounts for formatting differences between the datasets and allows "3500 Lowell Street" to be matched to "3500 N Lowell Street" which is correct. For each iteration, only those not matched in the previous iteration were tested with the lower scores. No score below 80 was used (80 was selected based on comparing visual matching to the fuzzy matching for a sample of the data)
