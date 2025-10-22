const express = require('express');
const AirtableService = require('../airtable/AirtableService');

// Usamos una función para exportar y pasar la instancia de 'base' de Airtable
module.exports = (base) => {
    const router = express.Router();
    
    // POST /migrateUser
    router.post('/migrateUser', async (req, res) => {
        try {
            // La validación de campos obligatorios se hace dentro del servicio
            const result = await AirtableService.migrateUser(base, req.body);

            res.status(201).json({
                message: `Usuario ${req.body.email} migrado y proyectos vinculados exitosamente.`,
                newUserId: result.newRecord.id,
                projectsCreated: result.projectsCreatedCount,
                fields: result.newRecord.fields
            });
            
        } catch (error) {
            console.error("Error en la migración del usuario:", error.message);
            // Manejamos las excepciones lanzadas por el servicio
            const statusCode = error.message.includes("no encontrado") ? 404 : 
                               error.message.includes("Debe proporcionar") ? 400 : 
                               500;
            res.status(statusCode).json({ error: error.message });
        }
    });

    // GET /user (Verificación de existencia)
    router.get('/user', async (req, res) => {
        try {
            const { email } = req.query;
            if (!email) {
                return res.status(400).json({ error: "Debe enviar un email como parámetro de consulta." });
            }
            
            const userRecord = await AirtableService.checkUserExists(base, email);

            if (!userRecord) {
                return res.status(404).json({ error: `Usuario con email ${email} no encontrado en la tabla de usuarios.` });
            }
            
            // Aquí agregaríamos la lógica de desnormalización si fuera necesario
            
            res.json({
                id: userRecord.id,
                fields: userRecord.fields
            });
        } catch (error) {
            console.error("Error al verificar usuario por email:", error);
            res.status(500).json({ error: "Error interno del servidor" });
        }
    });
    
    
    // PUT /user
   router.put('/user', async (req, res) => {
    try {
        const { email } = req.query;
        if (!email) {
            return res.status(400).json({ error: "Debe enviar un email como parámetro (?email=...)" });
        }

        // El servicio updateUser ahora devuelve el registro desnormalizado
        const updatedRecord = await AirtableService.updateUser(base, email, req.body);
        
        // La respuesta utiliza los campos del registro devuelto,
        res.json({
            message: `Usuario con email ${email} actualizado exitosamente.`,
            id: updatedRecord.id,
            fields: updatedRecord.fields
        });

    } catch (error) {
        console.error("Error al actualizar el usuario:", error.message);
        // Manejo de errores específicos del PUT
        const statusCode = error.message.includes("no puede estar vacío") ? 400 : 
                           error.message.includes("no encontrado") ? 404 : 
                           500;
        res.status(statusCode).json({ error: error.message });
    }
});

    // Rutas de Verificación (sin el prefijo /records)    
    // GET /user/fullName
    // Nota: El prefijo de esta ruta DEBE ser cambiado en server.js
    router.get('/user/fullName', async (req, res) => {
        try {
            const { email } = req.query;
            if (!email) {
                return res.status(400).json({ error: "Debe enviar un email como parámetro (?email=...)" });
            }
            
            const fullName = await AirtableService.getUserFullName(base, email);
            res.json({ fullName });
            
        } catch (error) {
            console.error("Error al obtener el nombre completo del usuario:", error.message);
            const statusCode = error.message.includes("no encontrado") ? 404 : 500;
            res.status(statusCode).json({ error: error.message });
        }
    });
    
    // GET /checkEmail
    // Nota: El prefijo de esta ruta DEBE ser cambiado en server.js
    router.get('/checkEmail', async (req, res) => {
        try {
            const { email } = req.query;
            if (!email) {
                return res.status(400).json({ error: "Debe enviar un email como parámetro (?email=...)" });
            }
            
            const exists = await AirtableService.checkEmailInPersonalTable(base, email);
            return res.json(exists); 
            
        } catch (error) {
            console.error("Error al verificar email:", error);
            res.status(500).json({ error: "Error interno del servidor" });
        }
    });

    router.get('/getalluser', async (req, res) => {
        try{
            const list = await AirtableService.getAllUsers(base);
            res.json(list);
        }
        catch (error){
            console.error("Error al obtener lista de Usuarios:", error);
            res.status(500).json({error: "Error interno del servidor"});
        }
    });

    router.get('/getallreferent', async (req, res) => {
        try{
            const list = await AirtableService.getAllReferent(base);
            res.json(list);
        }
        catch (error){
            console.error("Error al obtener lista de Referentes:", error);
            res.status(500).json({error: "Error interno del servidor"});
        }
    });

    router.get('/getallpartner', async (req, res) => {
        try{
            const list = await AirtableService.getAllPartner(base);
            res.json(list);
        }
        catch (error){
            console.error("Error al obtener lista de Talent Partners:", error);
            res.status(500).json({error: "Error interno del servidor"});
        }
    });

    router.get('/tecno', async(req, res) => {
        try{

            const { tec } = req.query;
            if (!tec) {
                return res.status(400).json({ error: "Debe enviar 'tec' como parámetro de consulta." });
            }

            const list = await AirtableService.getByTechnology(base, tec);
            res.json(list);
        }
        catch (error){
            console.error("Error al obtener lista de Usuarios por Tecnologia Actual", error);
            res.status(500).json({error: "Error interno del servidor"});
        }
    });
    return router;
};