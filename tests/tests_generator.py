import csv
from pathlib import Path
from src.lending_pipeline.generator import generate_data

def test_generator_counts_and_repeatability(tmp_path):
    a=tmp_path/"a"; b=tmp_path/"b"
    ca,aa=generate_data(a,5,12,123); cb,ab=generate_data(b,5,12,123)
    assert len(list(csv.DictReader(ca.open(encoding="utf-8")))) == 5
    assert len(list(csv.DictReader(aa.open(encoding="utf-8")))) == 12
    assert ca.read_bytes()==cb.read_bytes()
    assert aa.read_bytes()==ab.read_bytes()

def test_generator_rejects_bad_counts(tmp_path):
    import pytest
    with pytest.raises(ValueError): generate_data(tmp_path,0,5,1)
