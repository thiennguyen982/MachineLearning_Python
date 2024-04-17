# Databricks notebook source
import transformers

transformers.logging.set_verbosity_error()

from datasets import load_dataset, ClassLabel, Dataset
from transformers import (
    DataCollatorWithPadding,
    PhobertTokenizer,
    RobertaForSequenceClassification,
    RobertaModel,
)

from typing import Optional, List, Union
from torch.utils.data import DataLoader
import torch
from tqdm import tqdm
import pandas as pd
import numpy as np

# COMMAND ----------

class PacktypeClassifier:
    def __init__(
        self,
        packtype_labels: List = ["single", "multi", "combo"],
        load_path: Optional[str] = None,
        device: str = "cuda",
    ) -> None:
        self.labels = packtype_labels
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

    def get_packtype(
        self,
        input_dataset: pd.DataFrame,
        input_col: str = "TITLE",
        return_proba: bool = False,
        batch_size: int = 8,
    ) -> Union[np.ndarray, List]:
        """run classification model to classify product title to packtype

        Args:
            input_dataset (pd.DataFrame): input dataset include TITLE or SENT (TITLE + DESCRIPTION)
            ckpt_path (Optional[str], optional): the path to weight of trained model. Defaults to None.
            category_labels (List, optional): list of labels for setting order of model's output. Defaults to ["deo", "oral", "scl", "skin", "hair"].

        Returns:
            np.ndarray: the prediction returned by model.
        """
        # check the data
        if len(input_dataset) == 0:
            return None

        # init tokenizer
        tokenizer: PhobertTokenizer = get_tokenizer()

        # this class for convert string label to int
        labels = ClassLabel(num_classes=len(self.labels), names=self.labels)

        # convert pandas to huggingface's dataset class
        hf_input = Dataset.from_pandas(input_dataset)

        from functools import partial

        hf_input_encoded = hf_input.map(
            partial(preprocess_data, tokenizer=tokenizer, text_col=input_col),
            batched=True,
            remove_columns=hf_input.column_names,
        )
        hf_input_encoded.set_format("torch")

        # create dataloader
        data_collator = DataCollatorWithPadding(tokenizer=tokenizer)
        test_dataloader = DataLoader(
            hf_input_encoded,
            shuffle=False,
            batch_size=batch_size,
            collate_fn=data_collator,
        )

        # move model to CUDA
        self.model.to(self.device)

        # define a progress bar
        progress_bar = tqdm(range(len(test_dataloader)))

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
                    probas = torch.softmax(logits, dim=1).numpy()
                    if all_probas is None:
                        all_probas = probas
                    else:
                        all_probas = np.concatenate((all_probas, probas))

                if predictions is None:
                    predictions = outputs
                else:
                    predictions = np.concatenate((predictions, outputs))

            progress_bar.update(1)
        predictions = labels.int2str(predictions)
        input_dataset.loc[:, "PACKTYPE_PRED"] = predictions
        if return_proba:
            input_dataset.loc[
                :, [f"{col.upper()}_PROBA" for col in self.labels]
            ] = all_probas

            return input_dataset
        else:
            return input_dataset
