# Databricks notebook source
from typing import Dict
from datasets import ClassLabel

from transformers import PhobertTokenizer

# COMMAND ----------

def get_tokenizer(
    pretrained_model_name_or_path: str = "vinai/phobert-base-v2",
) -> PhobertTokenizer:
    """
    Create a PhoBERT tokenizer for tokenizing input text.

    Args:
        pretrained_model_name_or_path (str, optional): The name or path of the pretrained PhoBERT model.
            Defaults to "vinai/phobert-base-v2".

    Returns:
        PhobertTokenizer: A tokenizer instance for the specified PhoBERT model.
    """
    tokenizer = PhobertTokenizer.from_pretrained(
        pretrained_model_name_or_path=pretrained_model_name_or_path,
        cache_dir="../../models/",
    )
    return tokenizer


# define a tokenizer
# tokenizer = create_tokenizer()


# define a function for preprocessing data
# usage: for preprocess training and testing data.
def preprocess_data(
    examples: Dict, tokenizer: PhobertTokenizer, text_col: str = "TITLE"
):
    # encode them
    encoding = tokenizer(
        examples[text_col], padding="max_length", truncation=True, max_length=250
    )

    return encoding
