import React, { useState } from 'react';
import MapComponent from './MapComponent';

function App() {
  const [origin, setOrigin] = useState('');
  const [destination, setDestination] = useState('');
  const [travelMode, setTravelMode] = useState('');
  const [directions, setDirections] = useState(null);
  const [scores, setScores] = useState([]);

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
    // data['request'] = {
    //   "origin": {
    //       "query": origin
    //   },
    //   "destination": {
    //       "query": destination
    //   },
    //   "travelMode": travelMode
    // }
    // data.routes.forEach(route => { 
    //                                 route.legs.forEach(leg => { 
    //                                                             leg.steps.forEach(step=> {
    //                                                                                       step.path=window.google.maps.geometry.encoding.decodePath(step.polyline.points); }); }); });
    
    // route.bounds = { south: route.bounds.southwest.lat, west: route.bounds.southwest.lng, north: route.bounds.northeast.lat, east: route.bounds.northeast.lng };                                                                                       
    // console.log('response data Frontend', data)
    // let count = 0
    // let count2 = 0
    // data.routes[0].legs.forEach(leg => {
    //   count2 = count2 +leg.steps.length
    //   leg.steps.forEach(step => {
    //     count = count + step.path.length
    //   })
    // })
    // console.log("Route 1 paths", count, "steps", count2)

    // count = 0
    // count2 = 0
    // data.routes[1].legs.forEach(leg => {
    //   count2 = count2 +leg.steps.length
    //   leg.steps.forEach(step => {
    //     count = count + step.path.length
    //   })
    // })
    // console.log("Route 2 paths", count, "steps", count2)

    // count = 0
    // count2 = 0
    // data.routes[2].legs.forEach(leg => {
    //   count2 = count2 +leg.steps.length
    //   leg.steps.forEach(step => {
    //     count = count + step.path.length
    //   })
    // })
    // console.log("Route 3 paths", count, "steps", count2)
    setDirections(data)
  }

  const handleSelectChange = (event) => {
    setTravelMode(event.target.value);
  }

  return (
    <div className="App">
      <input 
        type="text" 
        placeholder="Origin" 
        value={origin}
        onChange={(e) => setOrigin(e.target.value)} 
      />
      <input 
        type="text" 
        placeholder="Destination" 
        value={destination}
        onChange={(e) => setDestination(e.target.value)} 
      />
      <select value={travelMode} onChange={handleSelectChange}>
          <option value="DRIVING">Driving</option>
          <option value="WALKING">Walking</option>
      </select>
      <button onClick={() => fetchDirections(origin, destination)}>Get Directions</button>
      <MapComponent directions={directions} scores={scores}/>
    </div>
  );
}

export default App;
