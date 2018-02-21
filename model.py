import tensorflow as tf

CSV_COLUMN_NAMES = ['horoscope', 'sign', 'month', 'day']
STAR_SIGNS = {1: 'aries',2: 'taurus',3: 'hemini',4: 'cancer',5: 'leo',6: 'virgo',7: 'libra',8: 'scorpio',9: 'sagittarius',10: 'capricorn',11: 'aquarius',12: 'pisces'}

def train_input_fn(features, labels, batch_size):
