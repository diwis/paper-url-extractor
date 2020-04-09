import sys
import csv
import urllib.request 
import time
import urllib.error
import random

# Define CSV dialect to be used.
csv.register_dialect(
    'exp_dialect',
    delimiter = '\t'
)

try:
    with open("articles.csv", 'r') as article_file, open("past_urls.csv",'r') as past_urls_file, open('urls.csv', 'w') as out_file, open('out-probl-net.csv','w') as net_file:
        article_reader = csv.reader(article_file,dialect='exp_dialect')
        past_urls_reader = csv.reader(past_urls_file,dialect='exp_dialect')
        out_writer = csv.writer(out_file,dialect='exp_dialect')
        net_writer = csv.writer(net_file,dialect='exp_dialect')
        #past_urls has three dictionaries, each having as key a different identifier (not all identifiers present in all entries)
        past_urls = [None] * 3
        past_urls[0] = dict() #by pmc = 0
        past_urls[1] = dict() #by doi = 1
        past_urls[2] = dict() #by pm = 2
        for url in past_urls_reader:
            if url[0]!="N/A":
                past_urls[0][url[0]] = url[3:]
            if url[1]!="N/A":
                past_urls[1][url[1]] = url[3:]
            if url[2]!="N/A":
                past_urls[2][url[2]] = url[3:]
        old_with_pmc = len(past_urls[0])
        old_with_doi= len(past_urls[1])
        old_with_pm = len(past_urls[2])
        print("Old articles: "+str(old_with_doi)+" with DOI, "+str(old_with_pmc)+" with PMC, "+str(old_with_pm)+" with PM.")
 
        art_with_doi = 0
        art_with_pmc = 0
        art_with_pm = 0
        art_all = 0
        url_ready = 0

        art_col_doi = 1 #doi column index in article file
        art_col_pmc = 0 #pmc column index in article file
        art_col_pm = 2 #pm column index in article file

        for entry in article_reader:
            art_all += 1
            entry_urls = [] #here will go URLs for this article
            entry_urls.extend(entry) #but first we should add the triplet of identifiers (doi-pmc-pm)

            #check if I already have it in the database
            was_ready_flag = False
            if entry[art_col_doi]!="N/A": #check by doi
                art_with_doi += 1
                if entry[art_col_doi] in past_urls[1]:
                    url_ready += 1
                    was_ready_flag = True
                    #TODO: CLEAN URL RULES GOES HERE!
                    entry_urls.extend(past_urls[1][entry[art_col_doi]]) #get the already retrieved URLs
                    print("[#"+str(art_all)+":] is ready (based on past data)")
                    #update properly the other two counts
                    if entry[art_col_pmc] in past_urls[0]:
                        art_with_pmc += 1
                    if entry[art_col_pm] in past_urls[2]:
                        art_with_pm += 1
            elif entry[art_col_pmc]!="N/A": #if not doi, check by pmc
                art_with_pmc += 1
                if entry[art_col_pmc] in past_urls[0]:
                    url_ready += 1
                    was_ready_flag = True
                    entry_urls.extend(past_urls[0][entry[art_col_pmc]]) #get the already retrieved URLs
                    print("[#"+str(art_all)+":] is ready (based on past data)")
                    #update properly the other count
                    if entry[art_col_pm] in past_urls[2]:
                        art_with_pm += 1
            elif entry[art_col_pm]!="N/A": #if not doi & pmc, check by pm
                art_with_pm += 1
                if entry[art_col_pm] in past_urls[2]:
                    url_ready += 1
                    was_ready_flag = True
                    entry_urls.extend(past_urls[2][entry[art_col_pm]]) #get the already retrieved URLs
                    print("[#"+str(art_all)+":] is ready (based on past data)")

            if was_ready_flag!=True: #this is a new article, thus collect URLs from scratch
                print("[#"+str(art_all)+":] is new")
                if entry[art_col_doi]!="N/A": #add doi-based urls
                    print("\tCollecting DOI-based urls...")
                    #add the two DOI links
                    entry_urls.append("http://doi.org/"+entry[art_col_doi])
                    entry_urls.append("http://dx.doi.org/"+entry[art_col_doi])
                    #try to add the redirection link (publisher's link)
                    try:
                        print("\tGet URL for entry: "+entry[art_col_doi] )
                        response = urllib.request.urlopen("http://doi.org/"+entry[art_col_doi],timeout=3)
                    except urllib.error.HTTPError as e:
                        #print(f'HTTPError: {e.code} ')
                        net_writer.writerow(entry)
                    except urllib.error.URLError as e:
                        #print(f'URLError: {e.code} ')
                        net_writer.writerow(entry)
                    except IOError as e:
                        #print(e)
                        net_writer.writerow(entry)
                    else:
                        print("\t["+entry[art_col_doi]+":] via '"+response.geturl()+"' code: "+str(response.getcode()))
                        new_url = response.geturl()
                        #clean a special case (nature error)
                        if( new_url.find("?error=") != -1 ):
                            first_occ = new_url.find("?error=")
                            new_url = new_url[:first_occ]
                        entry_urls.append(new_url)
                    sleep_time = 1+random.random()
                    print("\t[go to sleep:] "+str(sleep_time)+" sec")
                    time.sleep(sleep_time)
                if entry[art_col_pmc]!="N/A":
                    entry_urls.append("https://www.ncbi.nlm.nih.gov/pmc/articles/"+entry[art_col_pmc]) #add the corresponding PMC id
                if entry[art_col_pm]!="N/A":
                    entry_urls.append("https://www.ncbi.nlm.nih.gov/pubmed/"+entry[art_col_pm]) #add the corresponding PubMed id
            out_writer.writerow(entry_urls)
        print("All articles:\t"+str(art_all))
        print("With DOI:\t"+str(art_with_doi))
        print("With PMC:\t"+str(art_with_pmc))
        print("With PM:\t"+str(art_with_pm))
        print("Having already\t"+str(url_ready))
    article_file.close()
    past_urls_file.close()
    out_file.close()
    net_file.close()

except IOError as e:
    print("=> ERROR: Cannot open file...")
    #print(e)