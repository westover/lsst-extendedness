"""
Sample AVRO records for testing deserialization.

These dictionaries mimic the structure of LSST alert packets
as they would be deserialized from Kafka.
"""

# Sample AVRO record with SSObject
SAMPLE_AVRO_RECORD = {
    "alertId": 123456789,
    "diaSource": {
        "diaSourceId": 987654321,
        "diaObjectId": 111222333,
        "ra": 180.12345,
        "decl": 45.67890,
        "midPointTai": 60100.5,
        "filterName": "r",
        "psFlux": 1500.0,
        "psFluxErr": 15.0,
        "snr": 100.0,
        "extendednessMedian": 0.42,
        "extendednessMin": 0.38,
        "extendednessMax": 0.48,
        "trailLength": 0.0,
        "trailAngle": 0.0,
        "pixelFlagsBad": False,
        "pixelFlagsCr": False,
        "pixelFlagsEdge": False,
        "pixelFlagsSaturated": False,
    },
    "ssObject": {
        "ssObjectId": "SSO_2024_AB123",
        "ssObjectReassocTimeMjdTai": 60099.5,
    },
    "cutoutScience": b"FITS_DATA_PLACEHOLDER_SCIENCE",
    "cutoutTemplate": b"FITS_DATA_PLACEHOLDER_TEMPLATE",
    "cutoutDifference": b"FITS_DATA_PLACEHOLDER_DIFFERENCE",
}

# Sample AVRO record without SSObject
SAMPLE_AVRO_NO_SSO = {
    "alertId": 123456790,
    "diaSource": {
        "diaSourceId": 987654322,
        "diaObjectId": 111222334,
        "ra": 181.54321,
        "decl": 44.32100,
        "midPointTai": 60100.6,
        "filterName": "g",
        "psFlux": 2000.0,
        "psFluxErr": 20.0,
        "snr": 100.0,
        "extendednessMedian": 0.15,
        "extendednessMin": 0.12,
        "extendednessMax": 0.18,
        "trailLength": 0.0,
        "trailAngle": 0.0,
        "pixelFlagsBad": False,
        "pixelFlagsCr": True,
        "pixelFlagsEdge": False,
        "pixelFlagsSaturated": False,
    },
    "ssObject": None,
    "cutoutScience": b"FITS_DATA_PLACEHOLDER_SCIENCE",
    "cutoutTemplate": b"FITS_DATA_PLACEHOLDER_TEMPLATE",
    "cutoutDifference": b"FITS_DATA_PLACEHOLDER_DIFFERENCE",
}

# Sample AVRO record with trail (potential asteroid)
SAMPLE_AVRO_WITH_TRAIL = {
    "alertId": 123456791,
    "diaSource": {
        "diaSourceId": 987654323,
        "diaObjectId": 111222335,
        "ra": 182.00000,
        "decl": 43.00000,
        "midPointTai": 60100.7,
        "filterName": "i",
        "psFlux": 800.0,
        "psFluxErr": 12.0,
        "snr": 66.67,
        "extendednessMedian": 0.55,
        "extendednessMin": 0.45,
        "extendednessMax": 0.65,
        "trailLength": 15.5,
        "trailAngle": 45.2,
        "trailWidth": 2.3,
        "pixelFlagsBad": False,
        "pixelFlagsCr": False,
        "pixelFlagsEdge": False,
        "pixelFlagsSaturated": False,
    },
    "ssObject": {
        "ssObjectId": "SSO_2024_XY789",
        "ssObjectReassocTimeMjdTai": 60100.5,
    },
    "cutoutScience": b"FITS_DATA_PLACEHOLDER_SCIENCE",
    "cutoutTemplate": b"FITS_DATA_PLACEHOLDER_TEMPLATE",
    "cutoutDifference": b"FITS_DATA_PLACEHOLDER_DIFFERENCE",
}

# Sample extended source (galaxy)
SAMPLE_AVRO_EXTENDED = {
    "alertId": 123456792,
    "diaSource": {
        "diaSourceId": 987654324,
        "diaObjectId": 111222336,
        "ra": 183.00000,
        "decl": 42.00000,
        "midPointTai": 60100.8,
        "filterName": "z",
        "psFlux": 500.0,
        "psFluxErr": 25.0,
        "snr": 20.0,
        "extendednessMedian": 0.92,
        "extendednessMin": 0.85,
        "extendednessMax": 0.98,
        "trailLength": 0.0,
        "trailAngle": 0.0,
        "pixelFlagsBad": False,
        "pixelFlagsCr": False,
        "pixelFlagsEdge": True,
        "pixelFlagsSaturated": False,
    },
    "ssObject": None,
    "cutoutScience": b"FITS_DATA_PLACEHOLDER_SCIENCE",
    "cutoutTemplate": b"FITS_DATA_PLACEHOLDER_TEMPLATE",
    "cutoutDifference": b"FITS_DATA_PLACEHOLDER_DIFFERENCE",
}

# Collection of all sample records
ALL_SAMPLES = [
    SAMPLE_AVRO_RECORD,
    SAMPLE_AVRO_NO_SSO,
    SAMPLE_AVRO_WITH_TRAIL,
    SAMPLE_AVRO_EXTENDED,
]
