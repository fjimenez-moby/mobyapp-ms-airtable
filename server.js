// Cargar variables de entorno
require('dotenv').config();

// Módulos
const express = require('express');
const Airtable = require('airtable');
const userRoutes = require('./routes/userRoutes'); 

// --- Configuración de Airtable ---
const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;

// Asegúrate de que todas las variables de tabla estén definidas aquí o en un archivo de configuración
if (!AIRTABLE_API_KEY || !AIRTABLE_BASE_ID || !process.env.AIRTABLE_TABLE_NAME_PERSONALES || !process.env.AIRTABLE_TABLE_NAME_PROYECTOS || !process.env.AIRTABLE_TABLE_NAME_USERS_APP || !process.env.AIRTABLE_TABLE_NAME_PROYECTOS_APP) {
    console.error("Error: Las variables de entorno para Airtable no están configuradas correctamente.");
    process.exit(1);
}

// Inicializar la conexión con Airtable
const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

// Inicializar la aplicación Express
const app = express();
const PORT = process.env.PORT || 3000;

// Middleware para procesar JSON
app.use(express.json());

// --- Cargar Módulos de Rutas ---
// 1. Rutas que tienen el prefijo /api/airtable/records/... (POST/PUT/GET /user)
app.use('/api/airtable/records', userRoutes(base));

// 2. Rutas que tienen el prefijo /api/airtable/... (GET /checkEmail, /user/fullName)
// Reutilizamos userRoutes, ya que contiene toda la lógica. 
app.use('/api/airtable', userRoutes(base)); 


// Iniciar el servidor
app.listen(PORT, () => {
    console.log(`Microservicio de Airtable escuchando en el puerto ${PORT}`);
});