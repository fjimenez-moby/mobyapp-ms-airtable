// Lógica de negocio
const {
    AIRTABLE_TABLE_NAME_PERSONALES,
    AIRTABLE_TABLE_NAME_PROYECTOS,
    AIRTABLE_TABLE_NAME_USERS_APP,
    AIRTABLE_TABLE_NAME_PROYECTOS_APP
} = process.env;



/// Función auxiliar para mapear los nombres del DTO de Java a los nombres de Airtable
const mapFieldsToAirtable = (dtoFields) => {
    const mappedFields = {};

    // 1. Mapeo y validación de campos de texto simple (String)
    const simpleFieldsMap = {
        "name": "Nombre",
        "lastName": "Apellido",
        "province": "Provincia",
        "locality": "Localidad",
        "email": "Correo Moby",
        "currentTech": "Tecnologia Actual" // Incluido aquí para un manejo simple
    };

    for (const dtoKey in simpleFieldsMap) {
        const airtableKey = simpleFieldsMap[dtoKey];
        const value = dtoFields[dtoKey];

        // 🎯 CLAVE 1: Ignorar si el campo NO se envió (PUT parcial) o si es NULL.
        // Solo procesamos si el valor es EXISTENTE (no undefined ni null).
        if (value === undefined || value === null) {
            continue;
        }

        // Evitar la actualización del correo, ya que se usa como ID
        if (airtableKey === "Correo Moby") {
            continue;
        }

        if (typeof value === 'string') {
            const trimmedValue = value.trim();

            // 🎯 CLAVE 2: Validar solo si el campo es obligatorio Y se envió vacío ("")
            if ((airtableKey === "Nombre" || airtableKey === "Apellido") && trimmedValue === "") {
                throw new Error(`El campo '${airtableKey}' no puede estar vacío.`);
            }

            // Mapeo final: Enviamos el valor (incluso vacío, si no es obligatorio como Nombre/Apellido)
            mappedFields[airtableKey] = trimmedValue;
        }
        // Nota: Si el valor es de otro tipo (número, booleano) lo mapeamos directamente.
        // Si el valor es de otro tipo (ej. currentTech puede ser un array si lo mapeamos mal), deberías manejarlo aquí.
    }

    // 2. Mapeo de campos vinculados (Arrays de IDs) - Lógica dejada igual y es correcta
    const arrayFieldsMap = {
        "proyectos": "Proyectos",
        "referent": "Referente",
        "talentPartner": "Talent Partner"
    };

    for (const dtoKey in arrayFieldsMap) {
        const airtableKey = arrayFieldsMap[dtoKey];
        const value = dtoFields[dtoKey];

        if (Array.isArray(value)) {
            // Convierte la lista de IDs de String a formato de Airtable [{ id: '...' }]
            const idsPayload = value.map(id => ({ id: id }));

            // Siempre se mapea el array, incluso vacío, para actualizar/desvincular
            mappedFields[airtableKey] = idsPayload;
        }
    }

    return mappedFields;
};

// Funciones de Servicio
// POST /migrateUser

const escapeFormula = (val = '') => String(val).replace(/'/g, "\\'");

/**
 * Busca el primer record por igualdad exacta de un campo (texto o número).
 */
async function findRecordByField(base, tableName, fieldName, value) {
    return await base(tableName)
        .select({
            filterByFormula: `{${fieldName}} = '${escapeFormula(value)}'`,
            maxRecords: 1
        })
        .firstPage();
}

/**
 * Si tu campo "Cliente" en PROYECTOS_APP es LINK:
 *  - Pasá el nombre del cliente y resolvemos el ID en la tabla de clientes.
 *  - Si no existe, devolvemos null (y no seteamos el campo link).
 */
async function findClientIdByName(base, CLIENTES_TABLE, clientName) {
    if (!CLIENTES_TABLE || !clientName) return null;
    const res = await base(CLIENTES_TABLE)
        .select({
            filterByFormula: `{Nombre} = '${escapeFormula(clientName)}'`,
            maxRecords: 1
        })
        .firstPage();
    return res[0]?.id || null;
}

/**
 * Agrega un usuario a un proyecto SIN pisar los ya vinculados.
 */
async function addUserToProject(base, PROYECTOS_APP, projectId, newUserId) {
    const project = await base(PROYECTOS_APP).find(projectId);

    const currentLinks = Array.isArray(project.fields["Usuarios MobyApp"])
        ? project.fields["Usuarios MobyApp"]
        : [];

    // Normalizamos a ids string
    const currentIds = currentLinks
        .map(x => (typeof x === "string" ? x : x?.id))
        .filter(Boolean);

    const nextIds = Array.from(new Set([...currentIds, newUserId]));

    await base(PROYECTOS_APP).update([
        { id: projectId, fields: { "Usuarios MobyApp": nextIds } }
    ]);
}

/**
 * Obtiene (o crea) un proyecto en PROYECTOS_APP.
 * - Busca por Nombre + (ClienteNombre texto) para evitar comparar por link.
 * - Si querés buscar por link de Cliente, deberías usar una estrategia distinta.
 */
async function getOrCreateProject({
                                      base,
                                      PROYECTOS_APP,
                                      CLIENTES_TABLE,          // opcional: para resolver ID de cliente
                                      name,
                                      clientName,
                                      fechaInicio,
                                      fechaCierre,
                                      useClientLink = false,   // true si "Cliente" es link y querés setearlo
                                  }) {
    // 1) Buscamos por Nombre solamente (no usamos ClienteNombre)
    const byName = await base(PROYECTOS_APP)
        .select({
            filterByFormula: `{Nombre} = '${escapeFormula(name)}'`,
            maxRecords: 10
        })
        .firstPage();

    // 2) Si vamos a usar link de Cliente, resolvemos el ID (si se puede)
    let clientId = null;
    if (useClientLink && CLIENTES_TABLE && clientName) {
        const res = await base(CLIENTES_TABLE)
            .select({
                filterByFormula: `{Nombre} = '${escapeFormula(clientName)}'`,
                maxRecords: 1
            })
            .firstPage();
        clientId = res[0]?.id || null;
    }

    // 3) Si hay candidatos por nombre y tenemos clientId, tratamos de matchear
    if (byName.length && clientId) {
        const match = byName.find(r => {
            const link = r.fields["Cliente"];
            const ids = Array.isArray(link)
                ? link.map(x => (typeof x === "string" ? x : x?.id)).filter(Boolean)
                : [];
            return ids.includes(clientId);
        });
        if (match) return { id: match.id, created: false };
    }

    // 4) Si hay candidatos por nombre y NO tenemos clientId, podés reutilizar el primero
    //    (riesgo: varios proyectos con mismo nombre). Si preferís crear uno nuevo, salteá este bloque.
    if (byName.length && !clientId) {
        return { id: byName[0].id, created: false };
    }

    // 5) Crear el proyecto nuevo
    const fields = {
        "Nombre": name,
        "Fecha inicio": fechaInicio || undefined,
        "Fecha cierre": fechaCierre || undefined,
    };
    if (clientId) {
        fields["Cliente"] = [clientId]; // campo LINK correcto
    }

    const created = await base(PROYECTOS_APP).create([{ fields }]);
    return { id: created[0].id, created: true };
}

const migrateUser = async (base, reqBody) => {
    // Vars de entorno que ya tenías
    const {
        AIRTABLE_TABLE_NAME_PERSONALES,
        AIRTABLE_TABLE_NAME_PROYECTOS,
        AIRTABLE_TABLE_NAME_USERS_APP,
        AIRTABLE_TABLE_NAME_PROYECTOS_APP,
        AIRTABLE_TABLE_NAME_CLIENTES // opcional, solo si vas a linkear Cliente por ID
    } = process.env;

    const { email, nombre, apellido, foto } = reqBody;

    if (!email || !nombre || !apellido || !foto) {
        throw new Error("Debe proporcionar 'email', 'nombre' y 'apellido' y 'foto' en el cuerpo de la solicitud.");
    }

    const oldEmailFieldName = "Correo MOBY (from Datos Personales)";
    const oldLinkedFieldName = "Capacity";
    const newLinkedFieldName = "Proyectos";

    // 1) Buscar en Nómina Activa
    const personalRecords = await findRecordByField(base, AIRTABLE_TABLE_NAME_PERSONALES, oldEmailFieldName, email);
    if (personalRecords.length === 0) {
        throw new Error(`Usuario con email ${email} no encontrado en la nómina activa.`);
    }
    const personalRecord = personalRecords[0];
    const oldFields = personalRecord.fields;

    // 2) Preparar campos del usuario (sin nulls en links)
    const userFieldsToCreate = {
        "Correo Moby": email,
        "Nombre": nombre,
        "Apellido": apellido,
        "Foto de Perfil URL": foto || undefined,
        "Provincia": undefined,
        "Localidad": undefined,
        "Fecha de Alta": oldFields["Fecha de Alta (from Datos Personales)"] || undefined,
        "Tecnologia Actual": undefined,
        "Firma URL": undefined,
        "Es Talent Partner?": false,
        "Es Referente?": false,
        // Si estos son links, mejor omitirlos o usar []
        // "Referente": [],
        // "Talent Partner": [],
        [newLinkedFieldName]: []
    };

    // 3) Proyectos (del campo link "Capacity" de la tabla vieja)
    const projectRecordsIds = [];
    let projectsCreatedCount = 0;

    const hasOldProjects = Array.isArray(oldFields[oldLinkedFieldName]) && oldFields[oldLinkedFieldName].length > 0;
    if (hasOldProjects) {
        const projectIds = oldFields[oldLinkedFieldName];

        const oldProjectRecords = await Promise.all(
            projectIds.map(projectId => base(AIRTABLE_TABLE_NAME_PROYECTOS).find(projectId))
        );

        for (const p of oldProjectRecords) {
            const projectName   = p.fields["Proyectos"];
            const projectClient = p.fields["Cliente (from Oportunidades) (de Proyectos)"];
            const fechaInicio   = p.fields["Fecha de Asginacion"] || undefined;
            const fechaCierre   = p.fields["Fecha de Baja Servicio"] || undefined;

            if (!projectName) continue;

            // Crear/obtener en PROYECTOS_APP:
            const { id: newProjectId, created } = await getOrCreateProject({
                base,
                PROYECTOS_APP: AIRTABLE_TABLE_NAME_PROYECTOS_APP,
                CLIENTES_TABLE: AIRTABLE_TABLE_NAME_CLIENTES, // opcional
                name: projectName,
                clientName: projectClient,
                fechaInicio,
                fechaCierre,
                useClientLink: !!AIRTABLE_TABLE_NAME_CLIENTES // true si querés linkear por ID
            });

            if (created) projectsCreatedCount++;
            projectRecordsIds.push(newProjectId);
        }
    }

    userFieldsToCreate[newLinkedFieldName] = Array.from(new Set(projectRecordsIds.filter(Boolean)));

    // 4) Crear el usuario en USERS_APP
    const createdUser = await base(AIRTABLE_TABLE_NAME_USERS_APP).create([{ fields: userFieldsToCreate }]);
    const newUserId = createdUser[0].id;

    // 5) Vincular bidireccionalmente (sin pisar)
    if (projectRecordsIds.length > 0) {
        const uniqueProjectIds = Array.from(new Set(projectRecordsIds.filter(Boolean)));
        for (const projectId of uniqueProjectIds) {
            await addUserToProject(base, AIRTABLE_TABLE_NAME_PROYECTOS_APP, projectId, newUserId);
        }
    }

    // 6) Respuesta
    return {
        newRecord: createdUser[0],
        projectsCreatedCount
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
    if (referenteIds && referenteIds.length > 0) {
        const [referentRecord] = await lookupLinkedRecords( // Usa la NUEVA función
            base,
            referenteIds,
            AIRTABLE_TABLE_NAME_USERS_APP
        );
        // 🎯 En lugar de devolver un String, devolvemos el objeto con name, lastName, etc.
        updatedFields["Referente"] = referentRecord || null;
    } else {
        updatedFields["Referente"] = null; // Si no hay IDs, devuelve null
    }
    
    // c. Talent Partner (Vínculo Simple)
    const tpIds = updatedFields["Talent Partner"];
    if (tpIds && tpIds.length > 0) {
        const [tpRecord] = await lookupLinkedRecords( // Usa la NUEVA función
            base,
            tpIds,
            AIRTABLE_TABLE_NAME_USERS_APP
        );
        // 🎯 En lugar de devolver un String, devolvemos el objeto con name, lastName, etc.
        updatedFields["Talent Partner"] = tpRecord || null;
    } else {
        updatedFields["Talent Partner"] = null; // Si no hay IDs, devuelve null
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
        const error = new Error(`Usuario con email ${email} no encontrado.`);
        error.status = 404;
        throw error;
    }
    const userFields = records[0].fields;
    const nombre = userFields["Nombre"] || '';
    const apellido = userFields["Apellido"] || '';
    const fullName = `${nombre} ${apellido}`.trim();
    return fullName;
};

// GET /checkEmail (Verificación en nómina activa)
const checkEmailInPersonalTable = async (base, email) => {
    const records = await findRecordByField(base, AIRTABLE_TABLE_NAME_PERSONALES, "Correo MOBY (from Datos Personales)", email);

    return records.length !== 0;
     // Devolver true si el mail existe en la tabla personal
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

const lookupLinkedRecords = async (base, recordIds, tableName) => {
    if (!recordIds || recordIds.length === 0) {
        return [];
    }

    const filterFormula = recordIds.map(id => `RECORD_ID()='${id}'`).join(', ');
    const finalFormula = `OR(${filterFormula})`;

    try {
        const records = await base(tableName)
            .select({
                filterByFormula: finalFormula,
                // 🎯 Solicitamos todos los campos relevantes del UserDTO:
                fields: [
                    "Nombre", "Apellido", "Correo Moby", "Foto de Perfil URL",
                    "Provincia", "Localidad", "Tecnologia Actual",
                    "Fecha de Alta", "Firma URL"
                    // NOTA: No incluimos 'Referente' y 'Talent Partner' recursivamente.
                ]
            })
            .all();

        // Mapeamos los campos de Airtable (Español/Espacio) a la estructura camelCase de Java
        return records.map(record => ({
            // Campos base
            name: record.fields["Nombre"] || null,
            lastName: record.fields["Apellido"] || null,
            email: record.fields["Correo Moby"] || null,
            profilePicture: record.fields["Foto de Perfil URL"] || null,

            // Campos geográficos
            province: record.fields["Provincia"] || null,
            locality: record.fields["Localidad"] || null,

            // Campos de Rol y Fecha
            currentTech: record.fields["Tecnologia Actual"] || null,
            dateEntered: record.fields["Fecha de Alta"] || null, // Se envía como String YYYY-MM-DD
            signatureUrl: record.fields["Firma URL"] || null,

            // Campos que deben ser null en el anidado para evitar recursión infinita
            referent: null,     // Se pone a null manualmente
            talentPartner: null,// Se pone a null manualmente
            projects: null      // Se pone a null manualmente
        }));
    } catch (error) {
        console.error(`Error al buscar registros en la tabla ${tableName}:`, error.message);
        return [];
    }
};

const getUsers = async (base) => {
    // The Airtable equivalent of 'restTemplate.getForObject' to get all records
    const records = await base(AIRTABLE_TABLE_NAME_USERS_APP)
        .select({
        })
        .all(); // Fetches all pages of records

    if (!records || records.length === 0) {
        throw new Error("Usuarios no encontrados");
    }

    const users = records.map(record => ({
        name: record.fields["Nombre"] || null,
        lastName: record.fields["Apellido"] || null,
        email: record.fields["Correo Moby"] || null,
    }));

    return users;

};

const getReferents = async (base) => {
    const records = await base(AIRTABLE_TABLE_NAME_USERS_APP)
        .select({}).all();

    if (!records || records.length === 0) {
        throw new Error("Usuarios no encontrados");
    }
    const referents = records.map(record => ({
        email: record.fields["Correo Moby"] || null,
        name: record.fields["Nombre"] || null,
        lastName: record.fields["Apellido"] || null,
        ref: record.fields["Es Referente?"] || null

    })).filter(referent => referent.ref === true);

    return referents.map(({ ref, ...rest }) => rest);
};

const getPartners = async (base) => {
    const records = await base(AIRTABLE_TABLE_NAME_USERS_APP)
        .select({}).all();

    if (!records || records.length === 0) {
        throw new Error("Usuarios no encontrados");
    }

    const talentPartners = records.map(record => ({
        email: record.fields["Correo Moby"] || null,
        name: record.fields["Nombre"] || null,
        lastName: record.fields["Apellido"] || null,
        ref: record.fields["Es Talent Partner?"] || null

    })).filter(partner => partner.ref === true);
    return talentPartners.map(({ ref, ...rest }) => rest);
};

const getUsersByTechnology = async (base, technology) => {
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
        name: record.fields["Nombre"] || null,
        lastName: record.fields["Apellido"] || null,
        email: record.fields["Correo Moby"] || null,
    }));

    return users;
};

// Nuevo GET /user: Obtiene el UserDTO completo por email
const getUserByEmail = async (base, email) => {

    const userEmailFieldName = "Correo Moby";
    const records = await findRecordByField(base, AIRTABLE_TABLE_NAME_USERS_APP, userEmailFieldName, email);

    if (records.length === 0) {
        // Lanzar una excepción 404 para ser manejada por el controlador Express/JS
        const error = new Error(`Usuario con email ${email} no encontrado en la tabla de usuarios.`);
        error.status = 404;
        throw error;
    }

    const record = records[0];
    const userFields = record.fields; // Campos de Airtable

    // Creamos la estructura base del DTO que espera Java (camelCase)
    const userDTO = {
        name: userFields["Nombre"] || null,
        lastName: userFields["Apellido"] || null,
        email: userFields["Correo Moby"] || null,
        profilePicture: userFields["Foto de Perfil URL"] || null,
        province: userFields["Provincia"] || null,
        locality: userFields["Localidad"] || null,
        currentTech: userFields["Tecnologia Actual"] || null,
        dateEntered: userFields["Fecha de Alta"] || null,
        signatureUrl: userFields["Firma URL"] || null,

        // Campos vinculados inicializados a null o array vacío
        projects: [],
        referent: null,
        talentPartner: null
    };

    // 2. Desnormalización de Vínculos Múltiples (Projects)
    const projectIds = userFields["Proyectos"];
    if (projectIds) {
        // Usamos la función auxiliar existente para obtener los nombres de los proyectos
        userDTO.projects = await lookupLinkedNames(
            base,
            projectIds,
            AIRTABLE_TABLE_NAME_PROYECTOS_APP,
            "Nombre"
        );
    }

    // 3. Desnormalización de Vínculos Simples (Referent/Talent Partner)

    // a. Referente
    const referenteIds = userFields["Referente"];
    if (referenteIds && referenteIds.length > 0) {
        const [referentRecord] = await lookupLinkedRecords( // Usa la función robusta
            base,
            referenteIds,
            AIRTABLE_TABLE_NAME_USERS_APP
        );
        // El resultado ya es el UserReferenceDTO (name, lastName, email)
        userDTO.referent = referentRecord || null;
    }

    // b. Talent Partner
    const tpIds = userFields["Talent Partner"];
    if (tpIds && tpIds.length > 0) {
        const [tpRecord] = await lookupLinkedRecords( // Usa la función robusta
            base,
            tpIds,
            AIRTABLE_TABLE_NAME_USERS_APP
        );
        // El resultado ya es el UserReferenceDTO (name, lastName, email)
        userDTO.talentPartner = tpRecord || null;
    }
    return userDTO;
};

module.exports = {
    migrateUser,
    updateUser,
    checkUserExists,
    getUserFullName,
    checkEmailInPersonalTable,
    lookupLinkedNames,
    getUsers,
    getReferents,
    getPartners,
    getUsersByTechnology,
    getUserByEmail
};