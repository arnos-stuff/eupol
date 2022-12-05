import pandas as pd
from eupol.download.stats.eu import _toc
from eupol.download.text import tokenize, tokens

sample = pd.DataFrame.from_records([
    ( 'Technical and vocational education training (TVET)', 'MED_PS25', 'dataset', '2014-12-08T23:00:00+0100', '2021-02-08T23:00:00+0100', '2000', '2010'),
    ( 'Public expenditure on education', 'MED_PS26', 'dataset', '2020-05-28T23:00:00+0200', '2021-02-08T23:00:00+0100', '2005', '2018'),
    ( 'Poverty lines', 'MED_PS311', 'dataset', '2016-08-29T23:00:00+0200', '2021-02-08T23:00:00+0100', '2000', '2014'),
    ( 'At-risk-of poverty rate', 'MED_PS312', 'dataset', '2014-12-08T23:00:00+0100', '2021-02-08T23:00:00+0100', '2000', '2010'),
    ( 'Poverty ratio', 'MED_PS313', 'dataset', '2020-05-28T23:00:00+0200', '2021-02-08T23:00:00+0100', '2005', '2018'),
    ( 'Access to basic services and housing', 'MED_PS32', 'dataset', '2020-05-28T23:00:00+0200', '2021-02-08T23:00:00+0100', '2005', '2018'),
    ( 'Communication', 'MED_PS33', 'dataset', '2020-05-28T23:00:00+0200', '2021-02-08T23:00:00+0100', '2005', '2018'),
    ( 'Employment', 'MED_PS411', 'dataset', '2020-05-28T23:00:00+0200', '2021-02-08T23:00:00+0100', '2005', '2019'),
    ( 'Economic activity', 'MED_PS412', 'dataset', '2020-05-28T23:00:00+0200', '2021-02-08T23:00:00+0100', '2005', '2019'),
    ( 'Employment characteristics', 'MED_PS413', 'dataset', '2020-05-28T23:00:00+0200', '2021-02-08T23:00:00+0100', '2005', '2019'),
    ('Employment and economic activity branches', 'MED_PS414', 'dataset', '2020-05-28T23:00:00+0200', '2021-02-08T23:00:00+0100', '2005', '2019'),
    ('Unemployment rate', 'MED_PS421', 'dataset', '2020-05-28T23:00:00+0200', '2021-02-08T23:00:00+0100', '2005', '2019'),
    ('Unemployment rate by education level', 'MED_PS422', 'dataset', '2016-08-29T23:00:00+0200', '2021-02-08T23:00:00+0100', '2000', '2015'),
    ('Proportion of persons living in jobless households', 'MED_PS423', 'dataset', '2014-12-08T23:00:00+0100', '2021-02-08T23:00:00+0100', '2000', '2010'),
    ('Wages', 'MED_PS43', 'dataset', '2014-12-08T23:00:00+0100', '2021-02-08T23:00:00+0100', '2000', '2010'),
    ('Rail infrastructure: length of network', 'MED_RA1', 'dataset', '2020-05-28T23:00:00+0200', '2021-06-01T23:00:00+0200', '2005', '2019'),
    ('Rail equipment: number of locomotives and passenger railways vehicles', 'MED_RA2', 'dataset', '2020-05-28T23:00:00+0200', '2021-06-01T23:00:00+0200', '2005', '2019'),
    ('Rail equipment: number and capacity of railways vehicles', 'MED_RA3', 'dataset', '2020-05-28T23:00:00+0200', '2021-06-01T23:00:00+0200', '2005', '2019'),
    ('Rail equipment: number and load capacity of goods transport wagons', 'MED_RA4', 'dataset', '2020-05-28T23:00:00+0200', '2021-06-01T23:00:00+0200', '2005', '2019'),
    ('Rail passenger and freight traffic', 'MED_RA5', 'dataset', '2020-05-28T23:00:00+0200', '2021-06-01T23:00:00+0200', '2005', '2019'),
    ],
    columns=['title', 'code', 'type', 'last update of data', 'last table structure change', 'data start', 'data end'],
    )

sample_tokenize = pd.DataFrame.from_records([
    ('technical', 'vocational', 'education', 'training', 'tvet', '', '', ''),
    ('public', 'expenditure', 'education', '', '', '', '', ''),
    ('poverty', 'lines', '', '', '', '', '', ''),
    ('atriskof', 'poverty', 'rate', '', '', '', '', ''),
    ('poverty', 'ratio', '', '', '', '', '', ''),
    ('access', 'basic', 'services', 'housing', '', '', '', ''),
    ('communication', '', '', '', '', '', '', ''),
    ('employment', '', '', '', '', '', '', ''),
    ('economic', 'activity', '', '', '', '', '', ''),
    ('employment', 'characteristics', '', '', '', '', '', ''),
    ('employment', 'economic', 'activity', 'branches', '', '', '', ''),
    ('unemployment', 'rate', '', '', '', '', '', ''),
    ('unemployment', 'rate', 'education', 'level', '', '', '', ''),
    ('proportion', 'persons', 'living', 'jobless', 'households', '', '', ''),
    ('wages', '', '', '', '', '', '', ''),
    ('rail', 'infrastructure', 'length', 'network', '', '', '', ''),
    ('rail', 'equipment', 'number', 'locomotives', 'passenger', 'railways', 'vehicles', ''),
    ('rail', 'equipment', 'number', 'capacit', 'railways', 'vehicles', '', ''),
    ('rail', 'equipment', 'number', 'load', 'capacit', 'goods', 'transport', 'wagons'),
    ('rail', 'passenger', 'freight', 'traffic', '', '', '', ''),
    ])

sample_tokens = pd.DataFrame.from_records([
    ('rail', 5), ('employment', 3), ('rate', 3), 
    ('education', 3), ('equipment', 3), ('number', 3), ('poverty', 3), ('activity', 2), 
    ('railways', 2), ('vehicles', 2), ('passenger', 2), ('capacit', 2), ('economic', 2), 
    ('unemployment', 2), ('network', 1), ('infrastructure', 1), ('wages', 1), ('length', 1), 
    ('technical', 1), ('locomotives', 1), ('jobless', 1), ('load', 1), ('goods', 1), 
    ('transport', 1), ('wagons', 1), ('freight', 1), ('households', 1), ('level', 1), 
    ('living', 1), ('access', 1), ('training', 1), ('tvet', 1), ('public', 1), 
    ('expenditure', 1), ('lines', 1), ('atriskof', 1), ('ratio', 1), ('basic', 1), 
    ('persons', 1), ('services', 1), ('housing', 1), ('communication', 1), ('characteristics', 1), 
    ('branches', 1), ('vocational', 1), ('proportion', 1), ('traffic', 1)
    ],
    columns=['name', 'count']
    )

def test_tokenize():
    """Test the tokenize function."""
    tokenized = tokenize(sample, 'title')
    for (_, rA), (_, rB) in zip(tokenized.iterrows(), sample_tokenize.iterrows()):
        assert rA.equals(rB)
    
def test_tokens():
    """Test the tokens function."""
    tks = tokens(sample, 'title')
    for (_, rA), (_, rB) in zip(tks.iterrows(), sample_tokens.iterrows()):
        assert rA.equals(rB)
    # print(pd.concat(objs=[tks, sample_tokens], axis=1))


if __name__ == '__main__':
    test_tokenize()
    test_tokens()