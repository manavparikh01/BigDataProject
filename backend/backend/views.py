import requests
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from os import environ
import polyline
import pandas as pd

import googlemaps
from datetime import datetime

def process_routes(routes):
    for route in routes:
        # Update route bounds
        route['bounds'] = {
            'south': route['bounds']['southwest']['lat'],
            'west': route['bounds']['southwest']['lng'],
            'north': route['bounds']['northeast']['lat'],
            'east': route['bounds']['northeast']['lng']
        }
        
        # Decode polyline points in each step
        for leg in route['legs']:
            for step in leg['steps']:
                step['path'] = []
                latLngArray = polyline.decode(step['polyline']['points'])
                for latLng in latLngArray:
                    step['path'].append({"lat": latLng[0], "lng": latLng[1]})

def addRequestObject(data, origin, destination, travelMode):
    data['request'] = {
    "origin": {
        "query": origin
    },
    "destination": {
        "query": destination
    },
    "travelMode": travelMode
    }

def getCrimeScore(routes):
    scores = []

    data = pd.read_csv("//path to csv")
    
    for route in routes:
        count = 0
        for leg in route['legs']:
            for step in leg['steps']:
                for i in range(0, len(step['path']), 100):
                    lat = step['path'][i]['lat']
                    lon = step['path'][i]['lng']
                    for index, row in data.iterrows():
                        if row['Min Latitude'] <= lat <= row['Max Latitude'] and row['Min Longitude'] <= lon <= row['Max Longitude']:
                            if row['TOTAL_COUNT'] != None:
                                count = count + row['TOTAL_COUNT']
        scores.append(count)
    
    return scores
            
@require_http_methods(["GET"])
def get_directions(request):
    origin = request.GET.get('origin')
    destination = request.GET.get('destination')
    travelMode = request.GET.get('travelMode')
    api_key = '//Maps API Key'

    if not origin or not destination:
        return JsonResponse({'error': 'Missing parameters'}, status=400)

    url = f"https://maps.googleapis.com/maps/api/directions/json?origin={origin}&destination={destination}&mode={travelMode}&&key={api_key}&alternatives=true"
    response = requests.get(url)
    data = response.json()
    addRequestObject(data, origin, destination, travelMode)
    process_routes(data['routes'])
    scores = getCrimeScore(data['routes'])
    data['scores'] = scores

    return JsonResponse(data)
    
    # gmaps = googlemaps.Client(key=api_key)
    # now = datetime.now()
    # directions_result = gmaps.directions(origin,
    #                                  destination,
    #                                  mode="driving",
    #                                  departure_time=now, 
    #                                  alternatives=True)
    
    # return JsonResponse(directions_result, safe=False)