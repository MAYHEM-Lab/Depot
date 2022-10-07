import React from "react";
import {ReactWidget} from "@jupyterlab/ui-components";
import {UNSAFE_LocationContext as LocationContext, UNSAFE_NavigationContext as NavigationContext} from "react-router";
import DatasetCreator from "../dataset/creator"

export default class DatasetWidget extends ReactWidget {
    payload = null
    notebook = null

    constructor(notebook, location, navigator) {
        super()
        this.notebook = notebook
        this.location = location
        this.navigator = navigator
    }

    renderModel = (model) => {
        const data = model.data['application/depot-publish']
        const datasets = data.touched_datasets.map((info) => {
            const [entity, tag] = info.split('/')
            return {entity: entity, tag: tag}
        })
        this.payload = {
            touchedDatasets: datasets,
            resultType: data.result_type
        }
        return Promise.resolve()
    }

    render() {
        return <LocationContext.Provider value={this.location}>
            <NavigationContext.Provider value={this.navigator}>
                <DatasetCreator payload={this.payload} notebook={this.notebook}/>
            </NavigationContext.Provider>
        </LocationContext.Provider>
    }
}
