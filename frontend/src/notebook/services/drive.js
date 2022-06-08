import {Drive} from "@jupyterlab/services";
import API from "../../api";
import {NotebookModel} from "@jupyterlab/notebook/lib/model";
import {DepotKernel} from "./kernel";

export default class DepotDrive extends Drive {
    constructor(staticContent) {
        super();
        this.staticContent = staticContent
    }

    get = async (path) => {
        if (this.staticContent) {
            console.log('Serving static notebook')
            return {
                format: 'json',
                type: 'notebook',
                content: this.staticContent
            }
        }
        if (path.startsWith('#anonymous')) {
            console.log('Creating new notebook')

            const nb = new NotebookModel()
            nb.sharedModel.updateMetadata({
                kernelspec: DepotKernel
            })
            return {
                format: 'json',
                type: 'notebook',
                content: nb.toJSON()
            }
        }
        const [entity, tag] = path.split('/', 2)
        const content = await API.readNotebook(entity, tag)
        return {
            format: 'json',
            type: 'notebook',
            content: content
        }
    }
    rename = () => Promise.resolve(null)
    createCheckpoint = () => Promise.resolve(null)
    listCheckpoints = () => Promise.resolve([])
    deleteCheckpoint = () => Promise.resolve()
    restoreCheckpoint = () => Promise.resolve()
}
