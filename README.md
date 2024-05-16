# Ali_Auction
## Publication
SIGHCI 2023 Proceedings:
[When Online Auction Meets Virtual Reality: An Empirical Investigation estigation](https://aisel.aisnet.org/cgi/viewcontent.cgi?article=1000&context=sighci2023)
## Data
### source
[Ali auction website](https://sf.taobao.com/) historical auction records of houses

### Details
Collection requirements:
* Type of subject matter: residential houses
* Location of the subject matter: Suzhou, Wuxi, Hangzhou, Wenzhou, Hefei, Chengdu
* Type of asset: unlimited
* Auction status: terminated (main), suspended, withdrawn
* date: January 1, 2020 to June 30, 2022

## Crawling
### Workflow
![image](https://github.com/YyyyyyiZ/Ali_Auction/assets/109188165/d50bd41b-9bc0-4e29-9ddf-9719539ce864)

#### Crawling the listpage and detail page
> crawler_alfp.py

Crawl the specific content (including listpage and corresponding detail page) of the subject matter. First, modify line 347 of crawler_alfp_city.py to set crawler_list = True to collect listpage (need to slice), then set crawler_list = False for detail page collection. The second process takes a long time.

This part gets the listpage.csv, source.csv and html local files.
#### Parsing data
> parse_source.py

Run the parsing code to clean the fields based on source.csv and then get the parsed data. After all the pages are collected, modify the last line of the parse_source.py to change the parameter of run to the path of the existing source.csv, for parsing and normalization.

This part gets the std_city_final.csv file.
#### Downloading attachments
> get_file.py

After all the fields are parsed and standardized, run get_file.py to download attachments to local.
