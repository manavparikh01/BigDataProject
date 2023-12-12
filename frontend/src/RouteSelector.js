import React from 'react';

function RouteSelector({ routes, onRouteSelect, scores }) {
    return (
      <div>
        {routes.map((route, index) => (
          <div>
            <button key={index} onClick={() => onRouteSelect(index)}>
              Route {index + 1}
            </button>
            <p>Score: {scores[index]}</p>
          </div>
        ))}
      </div>
    );
  }

export default RouteSelector;