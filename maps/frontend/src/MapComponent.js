import React, { useState, useEffect, useRef } from 'react';
import { StandaloneSearchBox, GoogleMap, DirectionsRenderer } from '@react-google-maps/api';
import RouteSelector from './RouteSelector';

function MapComponent({mapLoaded}) {
  const [selectedRouteIndex, setSelectedRouteIndex] = useState(0);
  const [response, setResponse] = useState(false);
  const [origin, setOrigin] = useState('');
  const [destination, setDestination] = useState('');
  const [travelMode, setTravelMode] = useState('');
  const [directions, setDirections] = useState(null);
  const [scores, setScores] = useState([]);
  const originRef = useRef(null);
  const destinationRef = useRef(null);

  async function fetchDirections(origin, destination) {
    const encodedOrigin = encodeURIComponent(origin);
    const encodedDestination = encodeURIComponent(destination);
    const encodedTravelMode = encodeURIComponent(travelMode);

    console.log('origin', origin)
    console.log('destination', destination)
  
    const response = await fetch(`http://127.0.0.1:8000/get-directions/?origin=${encodedOrigin}&destination=${encodedDestination}&travelMode=${encodedTravelMode}`);
    let data = await response.json();
    console.log('response data Backend', data)
    let crimeScores = data.scores
    console.log('scores', crimeScores)
    setScores(crimeScores)
    delete data.scores
    setDirections(data)
  }

  const handleOriginChange = () => {
    if (originRef.current) {
      setOrigin(originRef.current.getPlaces()[0].formatted_address);
    }
  };

  const handleDestinationChange = () => {
    if (destinationRef.current) {
      setDestination(destinationRef.current.getPlaces()[0].formatted_address);
    }
  };

  const handleSelectChange = (event) => {
    setTravelMode(event.target.value);
  }

  const handleRouteSelect = (index) => {
    setSelectedRouteIndex(index);
  };

  const renderRoutes = () => {
    if (directions && mapLoaded) {
      console.log('inside render routes')
      console.log('directions', directions)
      return directions.routes.map((route, index) => {
        if (index === selectedRouteIndex) {
          console.log('passed directions, index =', index, {...directions, routes:[route]})
          return (
            <DirectionsRenderer 
              key={index}
              routeIndex={index}
              directions={{...directions, routes:[route]}}
              options={{ 
                polylineOptions: {
                  strokeColor: 'green',
                  strokeOpacity: 0.8,
                  strokeWeight: 6,
                },
                suppressMarkers: false
              }}
            />
          );
        }
        return null;
      }).filter(r => r != null);
    }
  };

  useEffect(() => {
    if (directions && mapLoaded) {
      console.log('inside useeffect')
      setResponse(renderRoutes())
    }
  }, [directions, selectedRouteIndex, mapLoaded]);

  return (
    <>
      <StandaloneSearchBox
          onLoad={ref => originRef.current = ref}
          controlPosition={10.0}
          onPlacesChanged={handleOriginChange}
      >
        <input
          type="text"
          placeholder="Origin"
          value={origin}
          onChange={(e) => setOrigin(e.target.value)}
          style={{ width: "200px", height: "32px", margin: "10px" }}
        />
      </StandaloneSearchBox>
      <StandaloneSearchBox
        onLoad={ref => destinationRef.current = ref}
        controlPosition={10.0}
        onPlacesChanged={handleDestinationChange}
      >
        <input
          type="text"
          placeholder="Destination"
          value={destination}
          onChange={(e) => setDestination(e.target.value)}
          style={{ width: "200px", height: "32px", margin: "10px" }}
        />
      </StandaloneSearchBox>
      <select value={travelMode} onChange={handleSelectChange}>
          <option value="DRIVING">Driving</option>
          <option value="WALKING">Walking</option>
      </select>
      <button onClick={() => {setDirections(null)
                              fetchDirections(origin, destination)}}
      >
        Get Directions
      </button>
      <GoogleMap
        mapContainerStyle={{ width: '100%', height: '500px' }}
        center={{ lat: -3.745, lng: -38.523 }}
        zoom={10}
      >
        {response}
      </GoogleMap>
      <RouteSelector routes={directions?.routes || []} onRouteSelect={handleRouteSelect} scores={scores}/>
    </>
  );
}

export default MapComponent;
