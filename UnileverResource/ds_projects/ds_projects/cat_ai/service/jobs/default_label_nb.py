# Databricks notebook source
import enum


class Category(enum.Enum):
    """
    Default value for Category Classification model
    """

    THREE_CAT = ["deo", "oral", "scl"]
    FIVE_CAT = ["deo", "oral", "scl", "skin", "hair"]
    SIX_CAT = ["deo", "hair", "hc", "oral", "scl", "skin"]
    EIGHT_CAT = ["deo", "fabclean", "fabenc", "hair", "hnh", "oral", "scl", "skin"]


class Format(enum.Enum):
    """
    Default values for Format for each category
    """

    ORAL = [
        "DENTAL HYGIENE",
        "DENTAL TOOLS",
        "ETB",
        "ETB - HEAD",
        "ETB KID",
        "MOUTHWASH",
        "OTHERS",
        "TOOTHBRUSH",
        "TOOTHPASTE",
        "WATER FLOSS",
        "WATER FLOSS - HEAD PICK",
        "WHITENING PRODUCT (CREAM)",
        "WHITENING PRODUCT (GEL)",
        "WHITENING PRODUCT (KIT)",
        "WHITENING PRODUCT (MACHINE)",
        "WHITENING PRODUCT (OTHERS)",
        "WHITENING PRODUCT (PEN)",
        "WHITENING PRODUCT (POWDER)",
        "WHITENING PRODUCT (STRIP)",
    ]
    DEO = [
        "AEROSOL",
        "CREAM",
        "GEL",
        "NATURAL SOLUTIONS",
        "POWDER",
        "ROLL-ON",
        "SERUM",
        "STICK",
        "WIPES",
    ]
    SCL = [
        "BATH & BODY OTHERS",
        "BATHING OTHERS",
        "BATHING SALT",
        "BATHING SCRUB",
        "HANDWASH",
        "SANITIZER",
        "SHOWER",
        "SOAP",
    ]
    SKIN = [
        "BODY CARE",
        "EXFO",
        "FACIAL CLEANSER",
        "FACIAL MOISTURIZER",
        "FACIAL SUNCREAM",
        "LOTION",
        "MAKEUP REMOVAL",
        "MASK",
        "SERUM & ESSENCE",
        "TONER",
    ]
    HAIR = ["DRY SP", "HC", "LO", "LO (CAPSULES)", "SP", "ST"]
