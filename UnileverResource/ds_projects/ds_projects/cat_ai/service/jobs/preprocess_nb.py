# Databricks notebook source
import unicodedata
import re
import unidecode


def standardize_sent(sent):  # lower case and normalize
    sent = sent.lower()
    sent = unicodedata.normalize("NFKC", sent).lower()
    return sent


def remove_noise_by_token(sent):  # Remove special character, packsize in sent
    """The objective of this function is remove noist text which can impact to model: packsize, non-text, text in square
    Note: Some of the text in square bracket contain importance keywork such as: combo, so, we need to detect text in square is combo or not,
    if yes: adding combo to title. See: combo_bracket function
    Some of the text contain number and text: 5nice => need to detect and keep these words
    """
    title_replace_token = ['" +"', "|", "  ", "'", "xa0", ",", "+", "_", "\n"]
    title_regex_arr = ["[\[|<].*?###[]|>]", "[^\w\s]", "[^\w\s,]"]
    find_string = re.compile("[a-z]")
    for r in title_regex_arr:
        sent = re.sub(r, "", sent)
    sent = sent.split(" ")
    for r in title_replace_token:
        sent = [s.replace(r, "") for s in sent]
    sent = [s for s in sent if len(s.replace(" ", "")) > 0]
    sent = [
        s for s in sent if len(find_string.findall(unidecode.unidecode(s))) > 0
    ]  # remove text with contain number and alphabet
    sent = " ".join(sent)
    return sent


def combo_bracket(
    sent,
):  # Before remove text in []. we need to detect "combo" appear on [] or not and auto take "combo" out
    text_in = re.search(r"\[([A-Za-z0-9_]+)\]", sent)
    if text_in == None:
        text_in = ""
    else:
        text_in = text_in.group(1)
    sent = re.sub(r"\[.*?\]", "", sent)
    if "combo" in text_in:
        if "combo" in sent:
            sent = sent
        else:
            sent = "combo" + " " + sent
    else:
        sent = sent
    return sent


def main_preprocess(sent):
    """
    preprocessing input text or title

    Input:
      title

    Return:
      preprocessed title.
    """
    sent = standardize_sent(sent)
    sent = remove_noise_by_token(sent)
    sent = combo_bracket(sent)
    sent = sent.replace("  ", " ")
    return sent

# COMMAND ----------

