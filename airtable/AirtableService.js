// Lógica de negocio
// Definición de variables de entorno para Airtable (las obtenemos una sola vez)
const {
    AIRTABLE_TABLE_NAME_PERSONALES,
    AIRTABLE_TABLE_NAME_PROYECTOS,
    AIRTABLE_TABLE_NAME_USERS_APP,
    AIRTABLE_TABLE_NAME_PROYECTOS_APP
} = process.env;

// Función auxiliar para buscar un registro por campo y valor (DRY)
const findRecordByField = async (base, tableName, fieldName, value) => {
    return base(tableName)
        .select({
            filterByFormula: `{${fieldName}} = '${value}'`,
            maxRecords: 1,
        })
        .all();
};

// Lógica de Mapeo y Actualización (PUT)
// Función auxiliar para mapear los nombres del DTO de Java a los nombres de Airtable
const mapFieldsToAirtable = (dtoFields) => {
    const mappedFields = {};

    // Mapeo y validación de campos de texto simple (String)
    const simpleFieldsMap = {
        "nombre": "Nombre",
        "apellido": "Apellido",
        "provincia": "Provincia",
        "localidad": "Localidad",
        "correoMoby": "Correo Moby"
    };

    for (const dtoKey in simpleFieldsMap) {
        const airtableKey = simpleFieldsMap[dtoKey];
        const value = dtoFields[dtoKey];

        if (value !== undefined) {
            // 1. Evita la actualización del correo
            if (airtableKey === "Correo Moby") {
                continue;
            }

            // 2. Validación de reglas de negocio para Nombre y Apellido
            if ((airtableKey === "Nombre" || airtableKey === "Apellido") && (!value || (typeof value === 'string' && value.trim() === ""))) {
                throw new Error(`El campo '${airtableKey}' no puede estar vacío.`);
            }

            // 3. Limpieza de campos de texto
            if (typeof value === 'string') {
                const trimmedValue = value.trim();
                if (trimmedValue !== "") {
                    mappedFields[airtableKey] = trimmedValue;
                }
            }
        }
    }

    // Mapeo de Tecnología Actual (currentTech)
    if (dtoFields.currentTech && dtoFields.currentTech.name && typeof dtoFields.currentTech.name === 'string') {
        const techValue = dtoFields.currentTech.name.trim();
        if (techValue !== "") {
            mappedFields["Tecnologia Actual"] = techValue; 
        }
    }

    // Mapeo de campos vinculados (Arrays de IDs)
    const arrayFieldsMap = {
        "proyectos": "Proyectos",
        "referent": "Referente",
        "talentPartner": "Talent Partner"
    };

    for (const dtoKey in arrayFieldsMap) {
        const airtableKey = arrayFieldsMap[dtoKey];
        const value = dtoFields[dtoKey];

        if (Array.isArray(value)) {
            const isValidArray = value.every(item => typeof item === 'string');
            if (isValidArray) {
                mappedFields[airtableKey] = value;
            } else if (value.length > 0) {
                 console.warn(`Advertencia: El campo ${airtableKey} (DTO: ${dtoKey}) contiene elementos no string y será ignorado.`);
            }
            else if (value.length === 0) {
                mappedFields[airtableKey] = [];
            }
        }
    }

    return mappedFields;
};

// Funciones de Servicio
// POST /migrateUser
const migrateUser = async (base, reqBody) => {
    // Obtenemos email, nombre, apellido, y pictureUrl del body
    const { email, nombre, apellido, foto } = reqBody; 
    
    if (!email || !nombre || !apellido || !foto) {
        // Lanzamos un error que el controlador atrapará como 400
        throw new Error("Debe proporcionar 'email', 'nombre' y 'apellido' y 'foto' en el cuerpo de la solicitud.");
    }

    const oldEmailFieldName = "Correo MOBY (from Datos Personales)";
    const oldLinkedFieldName = "Capacity";
    const newLinkedFieldName = "Proyectos";

    // Busca en la nómina activa
    const records = await findRecordByField(base, AIRTABLE_TABLE_NAME_PERSONALES, oldEmailFieldName, email);
    
    if (records.length === 0) {
        throw new Error(`Usuario con email ${email} no encontrado en la nómina activa.`);
    }

    const personalRecord = records[0];
    const oldFields = personalRecord.fields;

    // Mapeo de campos del Usuario (tu mapeo original)
    let userFieldsToCreate = {
        "Correo Moby": email,
        "Nombre": nombre,
        "Apellido": apellido,
        "Foto de Perfil URL": foto || null,
        "Provincia": null,
        "Localidad": null,
        "Fecha de Alta": oldFields["Fecha de Alta (from Datos Personales)"] || null,
        "Tecnologia Actual": null,
        "Firma URL": null,
        "Es Talent Partner?": false,
        "Es Referente?": false,
        "Referente": null,
        "Talent Partner": null,
        [newLinkedFieldName]: []
    };

    // LÓGICA DE PROYECTOS 
    let projectRecordsIds = [];
    let projectsCreatedCount = 0;

    if (oldFields[oldLinkedFieldName] && oldFields[oldLinkedFieldName].length > 0) {
        const projectIds = oldFields[oldLinkedFieldName];

        const oldProjectRecords = await Promise.all(
            projectIds.map(projectId => base(AIRTABLE_TABLE_NAME_PROYECTOS).find(projectId))
        );

        for (const p of oldProjectRecords) {
            const projectName = p.fields["Proyectos"];
            const projectClient = p.fields["Cliente (from Oportunidades) (de Proyectos)"];

            if (!projectName) continue;

            const projectFields = {
                "Nombre": projectName,
                "Fecha inicio": p.fields["Fecha de Asginacion"] || null,
                "Fecha cierre": p.fields["Fecha de Baja Servicio"] || null,
                "Cliente": projectClient || null,
            };

            const searchFormula = `AND({Nombre} = '${projectName}', {Cliente} = '${projectClient || ''}')`;

            const existingProjects = await base(AIRTABLE_TABLE_NAME_PROYECTOS_APP)
                .select({
                    filterByFormula: searchFormula,
                    maxRecords: 1,
                })
                .firstPage();

            let newProjectId;

            if (existingProjects.length > 0) {
                newProjectId = existingProjects[0].id;
            } else {
                const projectsToCreate = [{ fields: projectFields }];
                const createdRecords = await base(AIRTABLE_TABLE_NAME_PROYECTOS_APP).create(projectsToCreate);
                newProjectId = createdRecords[0].id;
                projectsCreatedCount++;
            }
            projectRecordsIds.push(newProjectId);
        }
    }

    userFieldsToCreate[newLinkedFieldName] = projectRecordsIds;

    const recordsToCreate = [{ fields: userFieldsToCreate }];
    const newRecords = await base(AIRTABLE_TABLE_NAME_USERS_APP).create(recordsToCreate);
    const newUserId = newRecords[0].id;

    // VINCULA BIDIRECCIONALMENTE PROYECTOS -> USUARIO
    if (projectRecordsIds.length > 0) {
        const updatesForProjects = projectRecordsIds.map(projectId => ({
            id: projectId,
            fields: {
                "Usuarios MobyApp": [{ id: newUserId }]
            }
        }));
        await base(AIRTABLE_TABLE_NAME_PROYECTOS_APP).update(updatesForProjects);
    }

    // Devolvemos el registro creado y el contador de proyectos creados
    return { 
        newRecord: newRecords[0], 
        projectsCreatedCount: projectsCreatedCount 
    };
};

// PUT /user
const updateUser = async (base, email, dtoFields) => {
    // Mapea y valida los campos (utiliza la función auxiliar)
    let fieldsToUpdate = mapFieldsToAirtable(dtoFields);

    if (Object.keys(fieldsToUpdate).length === 0) {
        throw new Error("Debe proporcionar al menos un campo válido para actualizar en el cuerpo de la solicitud.");
    }

    // Busca el registro del usuario por email para obtener su ID
    const userEmailFieldName = "Correo Moby";
    const records = await findRecordByField(base, AIRTABLE_TABLE_NAME_USERS_APP, userEmailFieldName, email);

    if (records.length === 0) {
        throw new Error(`Usuario con email ${email} no encontrado en la tabla de usuarios.`);
    }

    const userIdToUpdate = records[0].id;

    // Prepara y ejecuta la actualización
    const updatePayload = [{
        id: userIdToUpdate,
        fields: fieldsToUpdate
    }];

    // Ejecuta la actualización en Airtable
    const updatedRecords = await base(AIRTABLE_TABLE_NAME_USERS_APP).update(updatePayload);
    const updatedRecord = updatedRecords[0];
    const updatedFields = updatedRecord.fields;

    // Desnormalización

    // a. Proyectos (Vínculo Múltiple)
    const projectIds = updatedFields["Proyectos"];
    if (projectIds) {
        updatedFields["Proyectos"] = await lookupLinkedNames(
            base, 
            projectIds, 
            AIRTABLE_TABLE_NAME_PROYECTOS_APP, 
            "Nombre" // <-- Ajusta este nombre si tu campo de proyecto es diferente
        );
    }

    // b. Referente (Vínculo Simple)
    const referenteIds = updatedFields["Referente"];
    if (referenteIds) {
        const referenteNames = await lookupLinkedNames(
            base, 
            referenteIds, 
            AIRTABLE_TABLE_NAME_USERS_APP, 
            "Apellido" 
        );
        updatedFields["Referente"] = referenteNames.length > 0 ? referenteNames[0] : null;
    }
    
    // c. Talent Partner (Vínculo Simple)
    const tpIds = updatedFields["Talent Partner"];
    if (tpIds) {
        const tpNames = await lookupLinkedNames(
            base, 
            tpIds, 
            AIRTABLE_TABLE_NAME_USERS_APP, 
            "Apellido" 
        );
        updatedFields["Talent Partner"] = tpNames.length > 0 ? tpNames[0] : null;
    }

    // 3. --- FIN DE DESNORMALIZACIÓN ---
    
    // Devuelve el registro con los campos desnormalizados
    return updatedRecord;
};

// GET /user (Verificación de existencia)
const checkUserExists = async (base, email) => {
    const userEmailFieldName = "Correo Moby";
    const records = await findRecordByField(base, AIRTABLE_TABLE_NAME_USERS_APP, userEmailFieldName, email);

    if (records.length === 0) {
        return null;
    }
    
    return records[0];
};

// GET /user/fullName
const getUserFullName = async (base, email) => {
    const records = await findRecordByField(base, AIRTABLE_TABLE_NAME_USERS_APP, "Correo Moby", email);

    if (records.length === 0) {
        throw new Error(`Usuario con email ${email} no encontrado.`);
    }
    
    const user = records[0].fields;
    return `${user.Nombre || ''} ${user.Apellido || ''}`.trim();
};

// GET /checkEmail (Verificación en nómina activa)
const checkEmailInPersonalTable = async (base, email) => {
    const records = await findRecordByField(base, AIRTABLE_TABLE_NAME_PERSONALES, "Correo MOBY (from Datos Personales)", email);

    if (records.length === 0) {
        return false;
    }
    return true; // Devolver true si el mail existe en la tabla personal
};

//Funcion que desnormaliza los campos basándose en los IDs devueltos
const lookupLinkedNames = async (base, recordIds, tableName, nameField) => {
    if (!recordIds || recordIds.length === 0) {
        return [];
    }
    
    // Construye la fórmula OR para buscar múltiples IDs
    const filterFormula = recordIds.map(id => `RECORD_ID()='${id}'`).join(', ');
    const finalFormula = `OR(${filterFormula})`;
    
    try {
        const records = await base(tableName)
            .select({ 
                filterByFormula: finalFormula, 
                fields: [nameField] 
            })
            .all();
            
        return records.map(record => record.fields[nameField]).filter(name => name);
    } catch (error) {
        console.error(`Error al buscar nombres en la tabla ${tableName}:`, error.message);
        return [];
    }
};

const getAllUsers = async (base) => {
    // The Airtable equivalent of 'restTemplate.getForObject' to get all records
    const records = await base(AIRTABLE_TABLE_NAME_USERS_APP)
        .select({
            // You can optionally add fields array to optimize the query
            // fields: ["Nombre", "Apellido", "Correo Moby", ...]
        })
        .all(); // Fetches all pages of records

    // Check if the array of records is empty (equivalent to null/length check in Java)
    if (!records || records.length === 0) {
        // Equivalent to throwing a NullPointerException
        throw new Error("Usuarios no encontrados");
    }

    // The records are an array of Airtable Record objects. We map them
    // to a cleaner, DTO-like structure, retaining the record ID.
    // The field names used here match the Airtable column names from your other functions.
    const users = records.map(record => ({
        id: record.id,
        correoMoby: record.fields["Correo Moby"] || null,
        nombre: record.fields["Nombre"] || null,
        apellido: record.fields["Apellido"] || null,
        // Add other DTO fields here as needed
    }));

    // Equivalent to return Arrays.asList(users)
    return users;

};

const getAllReferent = async (base) => {
    // The Airtable equivalent of 'restTemplate.getForObject' to get all records
    const records = await base(AIRTABLE_TABLE_NAME_USERS_APP)
        .select({}).all(); // Fetches all pages of records

    // Check if the array of records is empty (equivalent to null/length check in Java)
    if (!records || records.length === 0) {
        // Equivalent to throwing a NullPointerException
        throw new Error("Usuarios no encontrados");
    }

    // Filter based on the "Es Referente?" to get the referents list
    const users = records.map(record => ({
        id: record.id,
        correoMoby: record.fields["Correo Moby"] || null,
        nombre: record.fields["Nombre"] || null,
        apellido: record.fields["Apellido"] || null,
        ref: record.fields["Es Referente?"] || null
        // Add other DTO fields here as needed
    })).filter(referent => referent.ref === true);

    //return records.map(record => record.fields[nameField]).filter(name => name);

    // Equivalent to return Arrays.asList(referents)
    return users;

};

const getAllPartner = async (base) => {
    // The Airtable equivalent of 'restTemplate.getForObject' to get all records
    const records = await base(AIRTABLE_TABLE_NAME_USERS_APP)
        .select({}).all(); // Fetches all pages of records

    // Check if the array of records is empty (equivalent to null/length check in Java)
    if (!records || records.length === 0) {
        // Equivalent to throwing a NullPointerException
        throw new Error("Usuarios no encontrados");
    }

    // Filter based on the "Es Referente?" to get the referents list
    const users = records.map(record => ({
        id: record.id,
        correoMoby: record.fields["Correo Moby"] || null,
        nombre: record.fields["Nombre"] || null,
        apellido: record.fields["Apellido"] || null,
        ref: record.fields["Es Talent Partner?"] || null
        // Add other DTO fields here as needed
    })).filter(referent => referent.ref === true);

    //return records.map(record => record.fields[nameField]).filter(name => name);

    // Equivalent to return Arrays.asList(referents)
    return users;
};

const getByTechnology = async (base, technology) => {
    if (!technology || typeof technology !== 'string') {
        throw new Error("Debe proporcionar una tecnología válida para la búsqueda.");
    }

    const searchString = technology.trim();

    // Partial Match (equivalent to .includes(string)):
    // FIND('java', {Tecnologia Actual}) returns a position > 0 if found.
    const filterFormula = `FIND('${searchString.toLowerCase()}', LOWER({Tecnologia Actual})) > 0`;
    // We use LOWER() on both the search string and the field to ensure case-insensitive search.

    const records = await base(AIRTABLE_TABLE_NAME_USERS_APP)
        .select({
            filterByFormula: filterFormula
        })
        .all();

    if (!records || records.length === 0) {
        throw new Error(`Usuarios con la tecnología que incluye '${searchString}' no encontrados`);
    }

    // 4. Map the Airtable records to the cleaner DTO structure
    const users = records.map(record => ({
        id: record.id,
        correoMoby: record.fields["Correo Moby"] || null,
        nombre: record.fields["Nombre"] || null,
        apellido: record.fields["Apellido"] || null,
        // Include other fields from your DTO structure here
    }));

    return users;
};

module.exports = {
    migrateUser,
    updateUser,
    checkUserExists,
    getUserFullName,
    checkEmailInPersonalTable,
    lookupLinkedNames,
    getAllUsers,
    getAllReferent,
    getAllPartner,
    getByTechnology
};