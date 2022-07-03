import React, {useEffect, useRef, useState} from 'react';
import {useOutletContext} from "react-router";
import API from "../api";
import {Loader} from "semantic-ui-react";
import {BoxPanel, Widget} from "@lumino/widgets";
import {ContentsManager, KernelAPI, ServiceManager, SessionAPI} from "@jupyterlab/services";
import {NotebookModelFactory, NotebookWidgetFactory} from "@jupyterlab/notebook";
import {editorServices} from "@jupyterlab/codemirror";
import {RenderMimeRegistry, standardRendererFactories} from "@jupyterlab/rendermime";
import DepotRegistry from "../notebook/services/registry";
import DepotDrive from "../notebook/services/drive";
import {DocumentManager} from "@jupyterlab/docmanager";
import {CodeCell} from '@jupyterlab/cells';


function createPanel(result) {
    const rendermime = new RenderMimeRegistry({
        initialFactories: standardRendererFactories
    })
    const mFactory = new NotebookModelFactory({})
    const wFactory = new NotebookWidgetFactory({
        name: 'Notebook',
        modelName: 'notebook',
        fileTypes: ['notebook'],
        preferKernel: false,
        canStartKernel: false,
        rendermime: rendermime,
        mimeTypeService: editorServices.mimeTypeService,
        shutdownOnClose: true
    })

    wFactory.editorConfig.code.lineNumbers = true
    wFactory.editorConfig.code.lineWrap = true
    wFactory.editorConfig.code.readOnly = true
    wFactory.notebookConfig.scrollPastEnd = false
    wFactory.notebookConfig.showHiddenCellsButton = false

    const docRegistry = new DepotRegistry()
    docRegistry.addModelFactory(mFactory)
    docRegistry.addWidgetFactory(wFactory)

    const drive = new DepotDrive(result, null)

    const contentsManager = new ContentsManager({
        defaultDrive: drive
    })

    const manager = new ServiceManager({
        contents: contentsManager
    })

    const docManager = new DocumentManager({
        registry: docRegistry,
        manager: manager,
        opener: {
            open: () => {
            }
        }
    })

    const widget = docManager.open('test')
    widget.toolbar.dispose()
    widget.content.handleEvent = null
    widget.content.contentFactory.createCodeCell = (editor) => {
        const cell = new CodeCell(editor).initializeState();
        const widgets = cell.children()._source
        cell.inputArea._prompt.addClass('dataset-code-readonly')
        const inputWidget = widgets[1]
        const inputCollapserWidget = inputWidget.children()._source[0]
        inputCollapserWidget.dispose()
        return cell
    }
    return widget
}

export default function DatasetCode() {
    const {entity, dataset} = useOutletContext();
    const [code, setCode] = useState(null)
    const panel = useRef(new BoxPanel())

    SessionAPI.listRunning = () => Promise.resolve([])
    KernelAPI.listRunning = () => Promise.resolve([])

    useEffect(() => {
        API.getDatasetCode(entity.name, dataset.tag).then(result => {
            result.cells = result.cells.map((c) => {
                c.source = [c.source]
                c.metadata.editable = false
                c.outputs = []
                c.execution_count = null
                return c
            })
            setCode(result)
            panel.current.addWidget(createPanel(result))
        })
    }, [entity, dataset])

    const renderNotebook = (element) => {
        if (panel.current) {
            if (element) {
                Widget.attach(panel.current, element)
            } else {
                Widget.detach(panel.current)
            }
        }
    }

    if (code === null) {
        return <Loader active/>
    } else {
        return <div id='dataset-code' ref={renderNotebook}/>
    }
}
