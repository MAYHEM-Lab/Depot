import {Signal} from "@lumino/signaling";
import {KernelSpecAPI} from "@jupyterlab/services";

export const DepotKernel = {
    name: 'depot',
    language: 'python',
    display_name: 'Depot'
}

export const DepotSpecs = {
    default: 'depot',
    kernelspecs: {depot: DepotKernel}
}

export default class DepotKernelSpecManager {
    connectionFailure = new Signal(this)
    specs = DepotSpecs
}

KernelSpecAPI.getSpecs = () => Promise.resolve(DepotSpecs)
