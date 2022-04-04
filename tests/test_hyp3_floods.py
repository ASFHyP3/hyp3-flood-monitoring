import hyp3_floods


def test_filter_hazards():
    hazards = [
        {'hazard_ID': 0, 'type_ID': 'FLOOD'},
        {'hazard_ID': 1, 'type_ID': 'foo'},
        {'hazard_ID': 2, 'type_ID': 'FLOOD'},
        {'hazard_ID': 3, 'type_ID': 'bar'},
        {'hazard_ID': 4, 'type_ID': 'baz'},
        {'hazard_ID': 5, 'type_ID': 'FLOOD'},
    ]
    filtered = [
        {'hazard_ID': 0, 'type_ID': 'FLOOD'},
        {'hazard_ID': 2, 'type_ID': 'FLOOD'},
        {'hazard_ID': 5, 'type_ID': 'FLOOD'},
    ]
    assert hyp3_floods.filter_hazards(hazards) == filtered


def test_get_aoi():
    hazard = {'latitude': 37.949, 'longitude': -90.4527}
    aoi = 'POINT(-90.4527 37.949)'
    assert hyp3_floods.get_aoi(hazard) == aoi
