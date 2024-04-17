# Databricks notebook source
# IMPORT MODULES
import transformers

transformers.logging.set_verbosity_error()

import torch
from torch.utils.data import DataLoader

# from torch.nn import functional as F
import numpy as np
import pandas as pd
from functools import partial

from transformers import (
    PhobertTokenizer,
    RobertaForSequenceClassification,
    DataCollatorWithPadding,
    RobertaModel,
)

from tqdm import tqdm
from datasets import Dataset, ClassLabel
from typing import Optional, Union, List
from loguru import logger

# COMMAND ----------

class FormatClassifier:
    def __init__(
        self,
        format_labels: List = Format.DEO.value,
        load_path: Optional[str] = None,
        device: str = "cuda",
    ) -> None:
        self.labels = format_labels
        self.load_path = load_path
        self.device = device

        # init model
        self.model: RobertaModel = RobertaForSequenceClassification.from_pretrained(
            pretrained_model_name_or_path="vinai/phobert-base-v2",
            num_labels=len(self.labels),
        )

        if load_path is not None:
            print(f"Loading weight from checkpoint...")
            # load weight for model
            load_weight(
                model=self.model,
                ckpt_path=self.load_path,
                device=device,
            )
        else:
            print("Not loading model!!!")

        print("All is ready!!!")

    def get_format(
        self,
        input_dataset: pd.DataFrame,
        input_col: str = "TITLE",
        return_proba: bool = False,
        batch_size: int = 8,
    ) -> Union[List[List[str]], np.ndarray]:
        """run format prediction

        Parameters
        ----------
        `input_dataset` (pd.DataFrame): input data\n
        `input_col` (str, optional): name of column that used for input to model. Defaults to "TITLE".\n
        `ckpt_path` (Optional[str], optional): path to weight of model. Defaults to None.\n
        `format_labels` (List[str], optional): list of ground truth formats.\n
        Returns
        ---------
            Union[List[List[str]], np.ndarray]: could be predicted FORMAT of each TITLE or probability of each
        """

        # check the data
        if len(input_dataset) == 0:
            return None

        # get tokenizer
        tokenizer: PhobertTokenizer = get_tokenizer()

        # convert pandas dataframe to huggingface dataset
        hf_data = Dataset.from_pandas(input_dataset)
        hf_data_encoded = hf_data.map(
            partial(preprocess_data, tokenizer=tokenizer, text_col=input_col),
            batched=True,
            remove_columns=hf_data.column_names,
        )
        hf_data_encoded.set_format("torch")

        # create label -> integer mapper
        labels = ClassLabel(num_classes=len(self.labels), names=self.labels)

        # Define the parameters for training loop
        data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

        # create dataloader use the proprocessed dataset
        test_dataloader = DataLoader(
            hf_data_encoded,
            shuffle=False,
            batch_size=batch_size,
            collate_fn=data_collator,
        )

        self.model.to(self.device)

        # define a progress bar
        step_progress = tqdm(range(len(test_dataloader)), leave=False)

        # running loop
        predictions = []
        all_probas = None
        for test_batch in test_dataloader:
            test_batch = {k: v.to(self.device) for k, v in test_batch.items()}
            with torch.no_grad():
                # feed input into the model
                test_outputs = self.model(**test_batch)

                # get logit -> put into cpu
                # -> detach from computational graph
                logits = test_outputs.logits.cpu().detach()
                outputs = (torch.sigmoid(logits) >= 0.5).to(torch.int)
                if return_proba:
                    probas = torch.sigmoid(logits).numpy()
                    if all_probas is None:
                        all_probas = probas
                    else:
                        all_probas = np.concatenate((all_probas, probas))

                for output in outputs:
                    index = np.where(output == 1)[0]
                    predictions.append(labels.int2str(index))
            step_progress.update(1)

        input_dataset.loc[:, "FORMAT_PRED"] = pd.Series(predictions).values
        if return_proba:
            input_dataset.loc[
                :, [f"{col.upper()}_PROBA" for col in self.labels]
            ] = all_probas

            return input_dataset
        else:
            return input_dataset
