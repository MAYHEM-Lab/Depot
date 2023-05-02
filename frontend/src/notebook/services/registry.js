import {DocumentRegistry} from "@jupyterlab/docregistry/lib/registry";

const NOTEBOOK_FILE_TYPE = {
    name: 'notebook',
    contentType: 'notebook',
    fileFormat: 'json'
}

export default class DepotRegistry extends DocumentRegistry {
    getFileType = () => NOTEBOOK_FILE_TYPE
    getFileTypeForModel = () => NOTEBOOK_FILE_TYPE
    getFileTypesForPath = () => [NOTEBOOK_FILE_TYPE]
}