import React, { useState, useEffect } from 'react';
import { GoogleMap, LoadScript, DirectionsRenderer } from '@react-google-maps/api';
import RouteSelector from './RouteSelector';

function MapComponent({ directions, scores }) {
  const google = window.google;
  const [selectedRouteIndex, setSelectedRouteIndex] = useState(0);
  const [response, setResponse] = useState(false);
  const [mapLoaded, setMapLoaded] = useState(false);

//   map = new google.maps.Map(document.getElementById("map"), {
//     center: { lat: -34.397, lng: 150.644 },
//     zoom: 8,
// });

  const handleRouteSelect = (index) => {
    setSelectedRouteIndex(index);
  };

  useEffect(() => {
    if (mapLoaded) {
      const directionsService = new google.maps.DirectionsService();
      directionsService.route({
        origin: '',
        destination: '',
        travelMode: google.maps.TravelMode.DRIVING,
      })//, (result, status) => {
      //   if (status === google.maps.DirectionsStatus.OK) {
      //     console.log('direction services response', result)
      //     setResponse2(result);
      //   } else {
      //     console.error(`Error fetching directions ${result}`);
      //   }
      // });
    }
  }, [mapLoaded]);

  const renderRoutes = () => {
    if (directions) {
      console.log('inside render routes')
      console.log('directions', directions)
      return directions.routes.map((route, index) => {
        console.log('passed directions', {...directions, routes:[route]})
        // if (index === selectedRouteIndex) {
          return (
            <DirectionsRenderer 
              key={index}
              directions={{...directions, routes:[route]}}
              options={{ 
                polylineOptions: {strokeColor: '#'+(Math.random()*0xFFFFFF<<0).toString(16)},
                suppressMarkers: true
              }}
            />
          );
        // }
        return null;
      }).filter(r => r != null);
    }
  };

  useEffect(() => {
    if (directions) {
      console.log('inside useeffect')
      setResponse(renderRoutes())
    }
  }, [directions, selectedRouteIndex]);

  return (
    <LoadScript googleMapsApiKey={process.env.REACT_APP_GMAPS_API_KEY} onLoad={() => setMapLoaded(true)}>
      <GoogleMap
        mapContainerStyle={{ width: '100%', height: '500px' }}
        center={{ lat: -3.745, lng: -38.523 }}
        zoom={10}
      >
        {response}
      </GoogleMap>
      <RouteSelector routes={directions?.routes || []} onRouteSelect={handleRouteSelect} scores={scores}/>
    </LoadScript>
  );
}

export default MapComponent;
