import React, {useContext, useEffect, useRef, useState} from 'react';
import {useOutletContext} from "react-router";
import {UserContext} from "../auth";
import {Link, useParams} from "react-router-dom";
import {RenderMimeRegistry, standardRendererFactories} from "@jupyterlab/rendermime";
import {NotebookModelFactory, NotebookWidgetFactory} from "@jupyterlab/notebook";
import {editorServices} from "@jupyterlab/codemirror";
import DepotRegistry from "./services/registry";
import DepotDrive from "./services/drive";
import {ContentsManager, KernelAPI, ServiceManager, SessionAPI} from "@jupyterlab/services";
import {DocumentManager} from "@jupyterlab/docmanager";
import {CodeCell} from "@jupyterlab/cells";
import {BoxPanel, Widget} from "@lumino/widgets";
import API from "../api";
import {Button, Container, Header, Icon, Loader} from "semantic-ui-react";
import DatasetIcon from "../dataset/icon";
import VisibilityInfo from "../dataset/visbility";

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

        widgets[1].children()._source[0].dispose()
        widgets[3].children()._source[0].dispose()
        cell.inputArea._prompt.dispose()
        return cell
    }
    return widget
}


export default function NotebookCode() {
    const {entity} = useOutletContext();
    const user = useContext(UserContext)
    const {notebookTag} = useParams()
    const [code, setCode] = useState(null)
    const panel = useRef(new BoxPanel())

    SessionAPI.listRunning = () => Promise.resolve([])
    KernelAPI.listRunning = () => Promise.resolve([])

    useEffect(() => {
        API.readNotebook(entity.name, notebookTag).then(result => {
            result.cells = result.cells.map((c) => {
                c.source = [c.source]
                c.metadata.editable = false
                return c
            })
            setCode(result)
            panel.current.addWidget(createPanel(result))
        })
    }, [entity, user, notebookTag])

    const renderNotebook = (element) => {
        if (panel.current) {
            if (element) {
                Widget.attach(panel.current, element)
            } else {
                Widget.detach(panel.current)
            }
        }
    }

    return <>
        <Header size='huge'>
            <Header.Content>
                <Icon name='file code outline'/>
                <Link to={`/${entity.name}`}>{entity.name}</Link>
                <span className='resource-divider'>/</span>
                <Link to={`/${entity.name}/notebooks/${notebookTag}`}>{notebookTag}</Link>
            </Header.Content>
            {/*<VisibilityInfo dataset={dataset}/>*/}
            <Header.Subheader>Notebook</Header.Subheader>
        </Header>
        {code == null ? <Loader active/> : <div id='dataset-code' ref={renderNotebook}/>}
    </>

}