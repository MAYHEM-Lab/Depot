const DEPOT_URL = "/api/"
const UPLOAD_URL = "/upload/"

const handleErrors = (response) => {
    if (!response.ok) throw new Error(response.statusText)
    return response
};

export default {

    getNotebooks: (owner) => {
        return fetch(`${DEPOT_URL}entity/${owner}/notebooks`)
            .then(handleErrors)
            .then(response => response.json())
    },

    createNotebook: (owner, tag) => {
        return fetch(`${DEPOT_URL}entity/${owner}/notebooks/${tag}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'}
        })
            .then(handleErrors)
    },

    saveNotebook: (owner, tag, content) => {
        return fetch(`${DEPOT_URL}entity/${owner}/notebooks/${tag}/contents`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({content: content})
        })
            .then(handleErrors)
    },

    getNotebook: (owner, tag) => {
        return fetch(`${DEPOT_URL}entity/${owner}/notebooks/${tag}`)
            .then(handleErrors)
            .then(response => response.json())
    },

    readNotebook: (owner, tag) => {
        return fetch(`${DEPOT_URL}entity/${owner}/notebooks/${tag}/contents`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getHomePage: () => {
        return fetch(`${DEPOT_URL}home`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getDatasets: (owner) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getDataset: (owner, tag) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getDatasetCollaborators: (owner, tag) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/collaborators`)
            .then(handleErrors)
            .then(response => response.json())
    },

    addDatasetCollaborator: (owner, tag, name) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/collaborators`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({collaborator_name: name, role: 'Member'})
        })
            .then(handleErrors)
    },

    delDatasetCollaborator: (owner, tag, name) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/collaborators`, {
            method: 'DELETE',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({collaborator_name: name})
        })
            .then(handleErrors)
    },

    canManageEntity: (entity) => {
        return fetch(`${DEPOT_URL}entity/${entity}/manage`)
            .then(handleErrors)
            .then(response => response.json())
    },

    canManageDataset: (owner, tag) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/manage`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getStats: (owner, tag) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/stats`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getSample: (owner, tag) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/sample`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getLineage: (owner, tag) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/lineage`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getProvenance: (owner, tag, version) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/segments/${version}/provenance`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getSegments: (owner, tag) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/segments`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getSegment: (owner, tag, version) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/segments/${version}`)
            .then(handleErrors)
            .then(response => response.json())
    },

    bundleSegment: (owner, tag, version) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/segments/${version}/download`)
            .then(handleErrors)
            .then(response => response.text())
    },

    materializeSegment: (owner, tag, version) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/segments/${version}/materialize`, {
            method: 'POST'
        })
            .then(handleErrors)
            .then(response => response.json())
    },

    getHistory: (owner, tag, version) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/segments/${version}/history`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getDatasetCode: (owner, tag) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/notebook`)
            .then(handleErrors)
            .then(response => response.json())
    },

    updateDataset: (owner, tag, description, visibility, frequency, retention) => {
        const body = {
            description: description,
            visibility: visibility,
            schedule: frequency,
            retention: retention
        }
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}`, {
            method: 'PATCH',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        })
            .then(handleErrors)
    },

    startFileUpload: (owner, parts, contentType) => {
        return fetch(`${DEPOT_URL}entity/${owner}/files`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({parts: parts, content_type: contentType})
        })
            .then(handleErrors)
            .then(response => response.json())
    },

    uploadFileChunk: (owner, uploadId, filename, partNumber, file, signal, uploadProgress, persistProgress) => {
        const formData = new FormData()
        formData.append('file', file)
        return fetch(`${UPLOAD_URL}entity/${owner}/files/${filename}?upload_id=${uploadId}&part_number=${partNumber}`, {
            method: 'PUT',
            signal: signal,
            headers: {'Content-Type': 'application/octet-stream'},
            body: file
        })
            .then(handleErrors)
            .then(async (response) => {
                const reader = response.body.getReader()
                let closed = false
                while (!closed) {
                    const {value, done} = await reader.read()
                    closed = done
                    if (value) {
                        const text = Buffer.from(value).toString('utf8').split('data: ')[1]
                        const data = JSON.parse(text)
                        persistProgress(data)
                    }
                }
            })
    },

    deleteFileUpload: (owner, uploadId, filename) => {
        return fetch(`${DEPOT_URL}entity/${owner}/files/${filename}`, {
            method: 'DELETE',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({upload_id: uploadId})
        })
            .then(handleErrors)
    },

    commitFileUpload: (owner, uploadId, filename) => {
        return fetch(`${DEPOT_URL}entity/${owner}/files/${filename}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({upload_id: uploadId})
        })
            .then(handleErrors)
    },

    createUnmanagedSegment: (owner, tag, files) => {
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}/upload`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({files: files})
        })
            .then(handleErrors)
    },

    createUnmanagedDataset: (owner, tag, description, datatype, visibility) => {
        const body = {
            description: description,
            content: {},
            datatype: datatype,
            visibility: visibility,
            origin: 'Unmanaged',
            storage_class: 'Weak',
            triggers: [],
            isolated: true,
            schedule: null,
            retention: null
        }
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        })
            .then(handleErrors)
    },

    createManagedDataset: (
        owner,
        tag,
        description,
        content,
        datatype,
        visibility,
        storageClass,
        sources,
        isolated,
        frequency,
        retention
    ) => {
        const body = {
            description: description,
            content: content,
            datatype: datatype,
            visibility: visibility,
            storage_class: storageClass,
            origin: 'Managed',
            triggers: sources.map(({entity, tag}) => {
                return {
                    entity_name: entity,
                    dataset_tag: tag
                }
            }),
            isolated: isolated,
            schedule: frequency,
            retention: retention
        }
        return fetch(`${DEPOT_URL}entity/${owner}/datasets/${tag}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        })
            .then(handleErrors)
    },

    createOrganization: (tag) => {
        return fetch(`${DEPOT_URL}entity/${tag}`, {
            method: 'POST'
        })
            .then(handleErrors)
    },

    validateDatasetTag: (tag) => {
        return fetch(`${DEPOT_URL}validate/dataset/${tag}`)
            .then(handleErrors)
            .then(response => response.json())
    },

    validateUsername: (username) => {
        return fetch(`${DEPOT_URL}validate/username/${username}`)
            .then(handleErrors)
            .then(response => response.json())
    },

    validateNotebookTag: (tag) => {
        return fetch(`${DEPOT_URL}validate/notebook/${tag}`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getEntity: (name) => {
        return fetch(`${DEPOT_URL}entity/${name}`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getMembers: (name) => {
        return fetch(`${DEPOT_URL}entity/${name}/members`)
            .then(handleErrors)
            .then(response => response.json())
    },

    addMember: (organization, name) => {
        return fetch(`${DEPOT_URL}entity/${organization}/members`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({member_name: name, role: 'Member'})
        })
            .then(handleErrors)
    },

    removeMember: (organization, name) => {
        return fetch(`${DEPOT_URL}entity/${organization}/members`, {
            method: 'DELETE',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({member_name: name})
        })
            .then(handleErrors)
    },

    getQuota: (name) => {
        return fetch(`${DEPOT_URL}entity/${name}/quota`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getClusters: (name) => {
        return fetch(`${DEPOT_URL}clusters/${name}`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getAuthorizedClusters: () => {
        return fetch(`${DEPOT_URL}clusters`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getEntityById: (id) => {
        return fetch(`${DEPOT_URL}entity?id=${id}`)
            .then(handleErrors)
            .then(response => response.json())
    },

    getAuthorizedEntities: () => {
        return fetch(`${DEPOT_URL}entity?authorized=true`)
            .then(handleErrors)
            .then(response => response.json())
    },

    search: (usersOnly, name) => {
        const param = usersOnly ? 'search_user' : 'search_all'
        return fetch(`${DEPOT_URL}entity?${param}=${name}`)
            .then(handleErrors)
            .then(response => response.json())
    },

    logout: () => {
        return fetch(`${DEPOT_URL}auth/logout`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'}
        })
            .then(handleErrors)
    },

    auth: () => {
        return fetch(`${DEPOT_URL}auth`)
            .then(handleErrors)
            .then(response => response.json())
    },

    githubUrl: () => {
        return fetch(`${DEPOT_URL}auth/github`)
            .then(handleErrors)
            .then(response => response.text())
    },

    githubAuth: (code) => {
        return fetch(`${DEPOT_URL}auth/github`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({code: code})
        })
            .then(handleErrors)
            .then(response => response.json())
    },

    githubRegister: (username, ghToken) => {
        return fetch(`${DEPOT_URL}auth/github/register`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({username: username, token: ghToken})
        })
            .then(handleErrors)
            .then(response => response.json())
    }
}
