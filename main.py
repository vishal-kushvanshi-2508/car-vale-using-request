


from extract_data import *
import time
from store_data_database import *
from car_detail import *
from specs_features_detail import *


def main():
    ## -------first operation-----------
    # # create table for kia_location_url
    # create_cars_url_url_table()
    # print("table and db create")

    # # extract_store_urls data  
    # extract_store_urls()


    ## -------second operation-----------
    # # fetch_kia_location_url_table data 
    # car_url_list = fetch_cars_url_table()

    # # crete table kia_detail
    # create_kia_detail_table()

    
    # # Create table 
    # create_car_detail_table()

    # # # call process_kia_data
    # fetch_car_detail(list_data= car_url_list)


    # ## -------third operation-----------
    variants_url_list = fetch_car_detail_table()

    # crate table 
    create_features_detail_table()

    # thread with
    run_threaded_fetch(variants_url_list)

    # -------fetch specs_features_detail without thread--------
    # specs_features_detail(list_data = variants_url_list)


    # # ----------------check_Highlights availabe or not ----------------
    # check_Highlights()











if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print("time different  : ", end - start)