import {Drive} from "@jupyterlab/services";
import API from "../../api";
import {NotebookModel} from "@jupyterlab/notebook/lib/model";
import {DepotKernel} from "./kernel";

export default class DepotDrive extends Drive {
    constructor(staticContent, user) {
        super();
        this.staticContent = staticContent
        this.user = user
    }

    get = async (path) => {
        if (this.staticContent) {
            return {
                format: 'json',
                type: 'notebook',
                content: this.staticContent
            }
        }
        if (path.startsWith('#anonymous')) {

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
        const content = await API.readNotebook(this.user.name, path)
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
