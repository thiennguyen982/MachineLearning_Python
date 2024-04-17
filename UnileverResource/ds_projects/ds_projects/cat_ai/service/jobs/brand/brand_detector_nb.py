# Databricks notebook source
from fuzzysearch import find_near_matches
from fuzzywuzzy import fuzz
import ray
from loguru import logger

# COMMAND ----------

def keyword_fuzzy_matching(sent, list_brand_keyword):
    """
    Objective:
        find all possible brand that can be present in the title.

    Input:
        sent: title
        list_brand_keyword: list of all brands that exist in master data.

    Return:
        keyword_matches_arr: list of all possible brands.
    """
    keyword_matches_arr = [
        {
            "keyword": str(keyword),
            "keyword_matches": find_near_matches(str(keyword), sent, max_l_dist=2),
        }
        for keyword in list_brand_keyword
    ]

    keyword_matches_arr = [
        {
            "keyword": keyword_obj["keyword"],
            "ratio_matches_max": max(
                [
                    fuzz.ratio(keyword_matches.matched, keyword_obj["keyword"])
                    for keyword_matches in keyword_obj["keyword_matches"]
                ]
            ),
        }
        for keyword_obj in keyword_matches_arr
        if len(keyword_obj["keyword_matches"]) != 0
    ]

    # return list of brand name that match with
    # ratio_mathes_max >= 85
    if len(keyword_matches_arr) > 0:
        max_value = max(keyword_matches_arr, key=lambda x: x["ratio_matches_max"])[
            "ratio_matches_max"
        ]
        if max_value > 85:
            keyword_matches_arr = [
                keyword["keyword"]
                for keyword in keyword_matches_arr
                if keyword["ratio_matches_max"] == max_value
            ]
        else:
            keyword_matches_arr = []

    else:
        keyword_matches_arr = []

    return keyword_matches_arr

# COMMAND ----------

def map_main_brand(brand_ls, brand_dict):  # mapping brand_prediction to brand_family
    """
    Objective:
        Map our predicted brand to brand family

    Input:
        brand_ls: list of our predicted brands.
        brand_dict: dictionary for mapping from brand -> brand family.

    Return:
        main_brand: list of new brand after mapping.
    """
    main_brand = [brand_dict[brand] for brand in brand_ls]
    main_brand = list(set(main_brand))
    return main_brand


def get_max_index(ls):
    """
    Objective:
        get list of max value index.
    Return:
        ls_m: list of maximum indices.
    """
    m = max(ls)
    ls_m = [i for i, j in enumerate(ls) if j == m]
    return ls_m

# COMMAND ----------

def replace_text(sent, phrase):
    """
    Objective:
        replace brand in title with brand with "_" repalce " "

    Return:
        sent: with brand name be replaced with new brand name with "_"
    """
    phrase_brand = phrase.replace(" ", "_")
    if phrase in sent:
        sent = sent.replace(phrase, phrase_brand)
    elif phrase + " " in sent:
        sent = sent.replace(phrase, phrase_brand)
    elif " " + phrase in sent:
        sent = sent.replace(phrase, phrase_brand)

    return sent


"""
Ex: đầu gội clear men sạch tóc
Step 1: Find brand family base on the brand prediction in layer 1 => Main_brand () [clear, clear men]
Step 2: Underscore brand in brand_family clear, clear_men
Step 3: Check brand in brand_family in sent or not => replace brand in sent by brand_family Ex: dầu gội clear_men
Step 4: spilt sent [dầu,gội,clear_men]
Step 5: Find the final brand in sent clear_men

"""


def select_main_brand(main_brand, sent):  # Find brand family in title
    """
    Objective:
        select final brand in the list of possible brands

    Return:
        return list of final brands with the highest matching score.
    """
    score = []
    result_brand = []
    brand_final = []
    ls_new_brand = []

    # sort the brand list by the number of sub-words
    main_brand.sort(key=lambda x: x.count(" ") + 1, reverse=True)

    for brand in main_brand:
        len_t = brand.count(" ") + 1
        if len_t > 1:
            # if brand name's length is greater than 1
            # replace brand name with new brand name with "_" seperate.
            title_phrase = replace_text(sent, brand)  # underscore text in sentence

            # check if brand_name is in title
            brand_ = brand.replace(" ", "_")
            if brand_ in title_phrase:
                # update current title to modified title (brand name with brand_name)
                sent = title_phrase
                brand = brand_

        # list include brands and modifed brands
        ls_new_brand.append(brand)

    sent = sent.split(" ")
    for brand in ls_new_brand:
        ls_score = []
        for t_ in brand.split(" "):
            ls_score.append(
                max([fuzz.ratio(t_, title_word) for title_word in sent])
            )  # Find brand in sent by fuzzzy score
        max_score = sum(ls_score) / len(ls_score)
        if max_score > 90:
            score.append(max_score)
            result_brand.append(brand)

    # get final brand out of list of possible brands
    if len(score) < 1:
        brand_final = "not-detected"
    else:
        if len(score) == 1:
            brand_final = result_brand
        else:
            max_index = get_max_index(score)
            for max_id in max_index:
                brand_final.append(result_brand[max_id])

        # post process to replace "_" with " "
        brand_final = list(set(brand_final))
        brand_final = [b.replace("_", " ") for b in brand_final]

    # return list of final brands
    return brand_final

# COMMAND ----------

def run_brand_detection(sent: str, list_brand_keyword: list, brand_dict: dict):
    """get only one final brand and other possible brand.

    Parameters
    ----------
    sent : str
        input text
    list_brand_keyword : list
        list of brand.
    brand_dict : dict
        mapper for brand name and brand family.

    Returns
    -------
    str
        brand_final: list of brands with the highest matching score.
        brand_suggest: this is in case we could not detect main brand in the title will return a list of brands with matching score is greater than 85.
    """
    ls_brand = keyword_fuzzy_matching(sent, list_brand_keyword)

    # map pred -> familiy
    main_brand = map_main_brand(ls_brand, brand_dict)
    if len(main_brand) < 1:
        brand_final = "not-detected"
        if len(ls_brand) >= 1:
            brand_suggest = ls_brand
        else:
            brand_suggest = "no suggestion"
        return brand_final, brand_suggest
    elif len(main_brand) == 1:
        brand_final = main_brand
        brand_suggest = "no suggestion"
        return brand_final, brand_suggest
    elif len(main_brand) > 1:
        print(main_brand)
        brand_final = select_main_brand(main_brand, sent)
        brand_suggest = "no suggestion"
        return brand_final, brand_suggest


def get_brand(
    skuid, sent: str, dict_ls_brand_cate: dict, dict_brand_cate: dict, category: str
):
    """run brand detection process

    Parameters
    ----------
    sent : str
        product's title
    dict_ls_brand_cate : dict
        dictionary with key is category and value is list of brands
    dict_brand_cate : dict
        dictionary with key is category and value is dictionary {predicted brand: family brand}
    category : str
        the predicted category
    """
    brand_final, brand_suggest = run_brand_detection(
        sent, dict_brand_cate[category], dict_ls_brand_cate[category]
    )
    return [skuid, brand_final, brand_suggest]


@ray.remote
def REMOTE_get_brand(
    skuid_list: list,
    sent_list: list,
    dict_ls_brand_cate: dict,
    dict_brand_cate: dict,
    category_list: list,
):
    final_result = [
        get_brand(
            skuid=skuid,
            sent=sent,
            dict_ls_brand_cate=dict_ls_brand_cate,
            dict_brand_cate=dict_brand_cate,
            category=category,
        )
        for skuid, sent, category in zip(skuid_list, sent_list, category_list)
    ]
    return final_result