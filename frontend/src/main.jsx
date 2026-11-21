import React from 'react';
import ReactDOM from 'react-dom/client';
// Self-hosted font (pinned @fontsource package, every axis): no font CDN requests from the public site
import '@fontsource-variable/recursive/full.css';
import App from './App.jsx';
import './App.css';

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
