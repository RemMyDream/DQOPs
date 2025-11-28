import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import DataQualityApp from './components/DataQualityApp';

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
    <DataQualityApp/>
  </React.StrictMode>
);