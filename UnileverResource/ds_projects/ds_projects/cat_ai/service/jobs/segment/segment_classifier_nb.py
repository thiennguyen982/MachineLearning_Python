# Databricks notebook source
# IMPORT MODULES
import transformers

transformers.logging.set_verbosity_error()

import torch
from torch.utils.data import DataLoader
import pandas as pd
import numpy as np
from functools import partial
from typing import Optional, Union, List

from transformers import (
    PhobertTokenizer,
    RobertaForSequenceClassification,
    RobertaModel,
    DataCollatorWithPadding,
)

from tqdm import tqdm
from datasets import Dataset, ClassLabel
from loguru import logger

# COMMAND ----------

class SegmentClassifier:
    def __init__(
        self,
        segment_labels: List = ["Men", "Women/ Unisex"],
        load_path: Optional[str] = None,
        device: str = "cuda",
    ) -> None:
        self.labels = segment_labels
        self.load_path = load_path
        self.device = device

        # init model
        self.model: RobertaModel = RobertaForSequenceClassification.from_pretrained(
            pretrained_model_name_or_path="vinai/phobert-base-v2",
            num_labels=len(self.labels),
        )

        if load_path is not None:
            print(f"Loading weight from checkpoint {load_path}...")
            # load weight for model
            load_weight(
                model=self.model,
                ckpt_path=self.load_path,
                device=device,
            )
        else:
            print("Not loading model!!!")

        print("All is ready!!!")

    def get_segment(
        self,
        input_dataset: pd.DataFrame,
        input_col: str = "TITLE",
        return_proba: bool = False,
    ) -> Union[np.ndarray, List[str]]:
        # check the data
        if len(input_dataset) == 0:
            return None

        # get tokenizer
        tokenizer: PhobertTokenizer = get_tokenizer()

        # this class for convert string label to int
        labels = ClassLabel(num_classes=len(self.labels), names=self.labels)

        # apply the preprocessing function to training and testing data
        hf_data = Dataset.from_pandas(input_dataset)

        hf_data_encoded = hf_data.map(
            partial(preprocess_data, tokenizer=tokenizer, text_col=input_col),
            batched=True,
            remove_columns=hf_data.column_names,
        )

        # set format to torch.Tensor -> for feeding these tensors to model
        hf_data_encoded.set_format("torch")

        # Define the parameters for training loop
        data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

        test_dataloader = DataLoader(
            hf_data_encoded, shuffle=False, batch_size=8, collate_fn=data_collator
        )

        self.model.to(self.device)

        # define a progress bar
        step_progress = tqdm(range(len(test_dataloader)), leave=False)

        self.model.eval()
        predictions = None
        all_probas = None
        for test_batch in test_dataloader:
            test_batch = {k: v.to(self.device) for k, v in test_batch.items()}
            with torch.no_grad():
                test_outputs = self.model(**test_batch)
                logits = test_outputs.logits.detach().cpu()
                outputs = logits.numpy().argmax(axis=1)

                if return_proba:
                    probas = torch.softmax(logits, dim=1)
                    if all_probas is None:
                        all_probas = probas
                    else:
                        all_probas = np.concatenate((all_probas, probas))

                if predictions is None:
                    predictions = outputs
                else:
                    predictions = np.concatenate((predictions, outputs))
            # update progress bar
            step_progress.update(1)
        predictions = labels.int2str(predictions)

        input_dataset.loc[:, "SEGMENT_PRED"] = predictions
        if return_proba:
            input_dataset.loc[
                :, [f"{col.upper()}_PROBA" for col in self.labels]
            ] = all_probas

            return input_dataset
        else:
            return input_dataset
