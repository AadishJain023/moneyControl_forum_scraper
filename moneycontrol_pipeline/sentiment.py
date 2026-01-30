import math
import re
from typing import Dict, Optional, Union



# use https://huggingface.co/Vansh180/FinBERT-India-v1
#       https://huggingface.co/kdave/FineTuned_Finbert    
#(more for indian contexts)
#code mixed language models can be used too
class SentimentAnalyzer:
    """
    Run sentiment scoring with VADER when available and fall back to a small
    rule-based lexicon when not. The output is compatible with many strategy
    inputs: compound score and a discrete label.
    """

    def __init__(self) -> None:
        self._vader = self._load_vader()

    def _load_vader(self):
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        except Exception:
            return None
        return SentimentIntensityAnalyzer()

    def score(self, text: str) -> Dict[str, Union[float, str]]:
        cleaned = " ".join(re.findall(r"[\\w']+", text or ""))
        if not cleaned:
            return {"compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0, "label": "neutral"}

        if self._vader:
            scores = self._vader.polarity_scores(cleaned)
            compound = scores.get("compound", 0.0)
            return {
                **scores,
                "label": label_from_compound(compound),
            }

        # Simple fallback if VADER is not installed
        scores = simple_lexicon_score(cleaned)
        compound = scores["compound"]
        return {**scores, "label": label_from_compound(compound)}


def label_from_compound(compound: float) -> str:
    if compound >= 0.05:
        return "positive"
    if compound <= -0.05:
        return "negative"
    return "neutral"


# Very small lexicon to allow offline scoring when VADER is missing.
POSITIVE_WORDS = {
    "buy",
    "long",
    "up",
    "bull",
    "bullish",
    "gain",
    "gains",
    "profit",
    "profits",
    "green",
    "strong",
    "beat",
    "beats",
    "outperform",
    "great",
    "good",
    "positive",
    "surge",
    "rally",
}

NEGATIVE_WORDS = {
    "sell",
    "short",
    "down",
    "bear",
    "bearish",
    "loss",
    "losses",
    "red",
    "weak",
    "miss",
    "missed",
    "underperform",
    "bad",
    "negative",
    "fall",
    "plunge",
    "crash",
}


def simple_lexicon_score(text: str) -> Dict[str, float]:
    tokens = [t.lower() for t in text.split()]
    total = max(len(tokens), 1)

    pos_hits = sum(1 for t in tokens if t in POSITIVE_WORDS)
    neg_hits = sum(1 for t in tokens if t in NEGATIVE_WORDS)
    neu_hits = total - pos_hits - neg_hits

    compound = 0.0
    if total:
        compound = (pos_hits - neg_hits) / math.sqrt(total)

    return {
        "compound": compound,
        "pos": pos_hits / total,
        "neg": neg_hits / total,
        "neu": max(neu_hits, 0) / total,
    }
