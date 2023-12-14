import React, { useState } from 'react';
import MapComponent from './MapComponent';
import { LoadScript } from '@react-google-maps/api';

function App() {
  const [mapLoaded, setMapLoaded] = useState(false);
  const [ libraries ] = useState(['places']);

  return (
    <div className="App">
      <LoadScript googleMapsApiKey={process.env.REACT_APP_GMAPS_API_KEY} onLoad={() => setMapLoaded(true)} libraries={libraries}>
        <MapComponent mapLoaded={mapLoaded}/>
      </LoadScript>
    </div>
  );
}

export default App;
