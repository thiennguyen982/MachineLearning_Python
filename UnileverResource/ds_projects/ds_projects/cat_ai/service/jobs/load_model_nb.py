# Databricks notebook source
# load checkpoint  model
import re
import torch
from typing import Optional, Tuple
from transformers import AutoModel


def load_weight(
    model: Optional[torch.nn.Module] = None,
    ckpt_path: Optional[str] = None,
    device: str = "cpu",
):  # -> Tuple[AutoModel, int]:
    """
    Load model weights from a checkpoint file.

    Args:
        model (transformers.AutoModel): The model to load weights into.
        ckpt_path (str, optional): Path to the checkpoint file. Defaults to None.
        device (str, optional): Device to load the model weights to (e.g., 'cpu' or 'cuda').
                                Defaults to 'cpu'.

    Returns:
        tuple: A tuple containing the loaded model and the previous training epoch.
    """

    # read and state_dict and load it to model
    state_dict = torch.load(ckpt_path, map_location=device)
    # load weight
    model.load_state_dict(state_dict)

    return model
