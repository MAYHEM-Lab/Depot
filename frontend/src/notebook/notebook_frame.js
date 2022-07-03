import React, {Component, useEffect, useState} from 'react';
import {ContentsManager, KernelAPI, ServiceManager, SessionAPI} from "@jupyterlab/services";
import {ServerConnection} from "@jupyterlab/services/lib/serverconnection";
import {DocumentManager} from "@jupyterlab/docmanager";
import {NotebookActions, NotebookModelFactory, NotebookWidgetFactory} from "@jupyterlab/notebook";
import {editorServices} from '@jupyterlab/codemirror';
import {RenderMimeRegistry, standardRendererFactories} from "@jupyterlab/rendermime";
import {BoxPanel, Widget} from "@lumino/widgets";

import '@jupyterlab/notebook/style/index.js';
import '@jupyterlab/ui-components/style/index.js';
import '@jupyterlab/cells/style/index.js';

import '@jupyterlab/theme-light-extension/style/theme.css';

import './notebook.css'

import API from '../api'
import {Button, Dropdown, Icon, Menu} from "semantic-ui-react";

import DepotKernelSpecManager from "./services/kernel";
import DepotRegistry from "./services/registry";
import DepotDrive from "./services/drive";

import NotebookStatus from "./notebook_status";
import NotebookTitle from "./notebook_title";
import NotebookCreator from "./notebook_creator";

import DatasetWidget from "./dataset_creator";
import {CodeCell} from "@jupyterlab/cells";

function ClusterSelector({onSelect, user}) {
    const [clusters, setClusters] = useState(null)
    const [selected, setSelected] = useState(null)
    useEffect(async () => {
        const {clusters} = await API.getAuthorizedClusters()
        const clusterInfo = clusters.map(({cluster, owner}) => {
            return {
                tag: cluster.tag,
                id: cluster.id,
                entityName: owner.name,
                entityType: owner.type,
                status: cluster.status
            }
        })
        setClusters(clusterInfo)
    }, [user])

    const select = (cluster) => {
        setSelected(cluster.id)
        onSelect(cluster.entityName, cluster.tag)
    }

    const renderItem = (cluster) => {
        if (!cluster) return null
        const active = cluster.status === 'Active'
        return <Dropdown.Item disabled={!active} key={cluster.id} value={cluster.id} onClick={() => select(cluster)}>
            <div className='cluster-select-header'>
                <Icon name='server'/>
                <span className='cluster-select-title'>
                    {cluster.tag}
                </span>
            </div>
            <div className='cluster-select-description'>
                <Icon name={cluster.entityType === 'User' ? 'user' : 'building'}/>
                {cluster.entityName}
            </div>
            <div className='cluster-select-description'>
                <Icon name='circle' color={active ? 'green' : 'orange'}/>
                {cluster.status}
            </div>
        </Dropdown.Item>
    }

    const trigger = <Button
        primary
        className='notebook-action-button'
        icon='add'
    />

    return <Dropdown trigger={trigger} icon={null} direction='left' loading={!clusters} value={selected}>
        <Dropdown.Menu>
            <Dropdown.Item disabled text={<span>Select a notebook executor</span>}/>
            {clusters ? clusters.map((cluster) => renderItem(cluster)) : null}
        </Dropdown.Menu>
    </Dropdown>
}

export default class NotebookFrame extends Component {
    boundElement = null
    anonymousNotebookId = 0
    panel = new BoxPanel()

    docRegistry = null
    contentsManager = null

    docManagers = new Map()

    state = {
        notebookCreator: false,
        notebooks: [],
        activeNotebookIdx: -1
    }

    constructor(props) {
        super(props)
        const {location, navigator, user} = props

        SessionAPI.listRunning = () => Promise.resolve([])
        KernelAPI.listRunning = () => Promise.resolve([])

        const rendermime = new RenderMimeRegistry({
            initialFactories: standardRendererFactories
        })

        rendermime.addFactory({
            safe: true,
            mimeTypes: ['application/depot-publish'],
            createRenderer: () => {
                const {notebooks, activeNotebookIdx} = this.state;
                if (activeNotebookIdx !== -1) {
                    return new DatasetWidget(notebooks[activeNotebookIdx], location, navigator)
                }
            }
        })

        const mFactory = new NotebookModelFactory({})
        const wFactory = new NotebookWidgetFactory({
            name: 'Notebook',
            modelName: 'notebook',
            fileTypes: ['notebook'],
            defaultFor: ['notebook'],
            preferKernel: true,
            canStartKernel: true,
            rendermime: rendermime,
            mimeTypeService: editorServices.mimeTypeService,
            shutdownOnClose: true
        })

        wFactory.editorConfig.code.lineNumbers = true
        wFactory.notebookConfig.scrollPastEnd = false

        this.docRegistry = new DepotRegistry()
        this.docRegistry.addModelFactory(mFactory)
        this.docRegistry.addWidgetFactory(wFactory)

        const drive = new DepotDrive(null, user)

        this.contentsManager = new ContentsManager({
            defaultDrive: drive
        })
    }

    componentDidMount() {
        window.addEventListener('resize', () => this.panel.update())
        window.addEventListener('beforeunload', () => {
            this.panel.dispose()
            this.state.notebooks.forEach((nb) => nb.widget.dispose())
        })
        // this.openNotebook('d', 'm', 'KC-test')
    }

    getDocManager = (entity, cluster) => {
        if (this.docManagers.has(cluster)) return this.docManagers.get(entity + '/' + cluster)

        const settings = ServerConnection.makeSettings({
            baseUrl: '/notebook/' + entity + '/' + cluster,
        })

        const manager = new ServiceManager({
            serverSettings: settings,
            contents: this.contentsManager,
            kernelspecs: new DepotKernelSpecManager()
        })

        const docManager = new DocumentManager({
            registry: this.docRegistry,
            manager: manager,
            opener: {
                open: () => {
                }
            }
        })
        this.docManagers.set(entity + '/' + cluster, docManager)
        return docManager
    }

    openWidget = (entity, cluster, id) => {
        const widget = this.getDocManager(entity, cluster).open(id)
        widget.toolbar.dispose()
        widget.content.contentFactory.createCodeCell = (editor) => {
            const cell = new CodeCell(editor).initializeState();
            const widgets = cell.children()._source
            widgets[1].children()._source[0].dispose()
            widgets[3].children()._source[0].dispose()
            return cell
        }
        return widget
    }

    newNotebook = (entity, cluster) => {
        const {notebooks, activeNotebookIdx} = this.state
        if (activeNotebookIdx !== -1) {
            notebooks[activeNotebookIdx].widget.hide()
        }
        console.log('Opening new')
        const id = '#anonymous_' + this.anonymousNotebookId++
        const widget = this.openWidget(entity, cluster, id)
        const notebook = {
            widget: widget,
            entity: entity,
            cluster: cluster,
            local: true,
            id: id
        }
        notebook.widget.show()
        this.panel.addWidget(notebook.widget)
        this.setState({
            notebooks: [...notebooks, notebook],
            activeNotebookIdx: notebooks.length
        })
    }

    openNotebook = (notebookId, entity, cluster) => {
        const {notebooks, activeNotebookIdx} = this.state

        let existingIdx = notebooks.findIndex((nb) => nb.id === notebookId && nb.entity === entity && nb.cluster === cluster)

        if (existingIdx !== activeNotebookIdx)
            if (activeNotebookIdx !== -1) {
                notebooks[activeNotebookIdx].widget.hide()
            }

        let notebook
        if (existingIdx === -1) {
            const widget = this.openWidget(entity, cluster, notebookId)
            this.panel.addWidget(widget)

            notebook = {
                widget: widget,
                entity: entity,
                cluster: cluster,
                notebookId: notebookId,
                id: notebookId
            }
            this.setState({notebooks: [...notebooks, notebook]})
            existingIdx = notebooks.length
        } else {
            notebook = notebooks[existingIdx]
        }
        notebook.widget.show()

        this.setState({activeNotebookIdx: existingIdx})
    }

    closeNotebook = (notebookId) => {
        const {notebooks, activeNotebookIdx} = this.state

        const notebook = notebooks.find((nb) => nb.id === notebookId)
        console.log('Closing notebook ', notebook)

        if (notebook) {
            notebook.widget.dispose()
            const newNotebooks = notebooks.filter((nb) => nb.id !== notebookId)
            const openIdx = activeNotebookIdx === notebooks.length - 1 ? activeNotebookIdx - 1 : activeNotebookIdx
            if (openIdx >= 0) {
                notebooks[openIdx].widget.show()
                this.setState({activeNotebookIdx: openIdx})
            } else {
                this.setState({activeNotebookIdx: -1})
            }

            this.setState({notebooks: newNotebooks})
        }
    }

    saveOrCreate = async (notebook) => {
        const {user} = this.props
        if (notebook.local) {
            console.log('Creating notebook')
            this.setState({notebookCreator: true})
        } else {
            console.log('Saving notebook')
            await API.saveNotebook(user.name, notebook.id, notebook.widget.content.model.toJSON())
        }
    }

    handleNotebookCreate = async (notebookId, tag) => {
        const {user} = this.props
        const notebook = this.state.notebooks.find((nb) => nb.id === notebookId)
        if (notebook) {
            console.log(`Saving anonymous notebook ${notebookId} as ${tag}`)
            await API.createNotebook(user.name, tag)
            const existingNotebooks = this.state.notebooks
            const existingNotebook = existingNotebooks.find((nb) => nb.id === notebookId)
            if (existingNotebook) {
                await existingNotebook.widget.context.rename(tag)
                existingNotebook.id = tag
                existingNotebook.local = false
                await API.saveNotebook(user.name, tag, existingNotebook.widget.content.model.toJSON())
                this.setState({notebooks: existingNotebooks, notebookCreator: false})
            }
        }
    }

    notebookKeyHandler = async (event) => {
        const {notebooks, activeNotebookIdx} = this.state

        if (activeNotebookIdx !== -1) {
            const activeNotebook = notebooks[activeNotebookIdx]
            const widget = activeNotebook.widget

            if (event.ctrlKey && event.key.toLowerCase() === 's') {
                event.preventDefault()
                event.stopPropagation()
                await this.saveOrCreate(activeNotebook)
            }
            if (event.ctrlKey && event.key.toLowerCase() === 'd') {
                event.preventDefault()
                event.stopPropagation()
                NotebookActions.deleteCells(widget.content)
            }
            if (event.ctrlKey && event.key === 'Enter') {
                event.preventDefault()
                event.stopPropagation()
                await NotebookActions.run(widget.content, widget.context.sessionContext)
            }
            if (event.shiftKey && event.key === 'Enter') {
                event.preventDefault()
                event.stopPropagation()
                await NotebookActions.runAndAdvance(widget.content, widget.context.sessionContext)
            }
        }
    }

    componentWillUnmount() {
        const {notebooks} = this.state
        notebooks.forEach((nb) => nb.widget.dispose())
        if (this.panel.isAttached) {
            Widget.detach(this.panel)
        }
        if (this.boundElement) {
            this.boundElement.removeEventListener('keydown', this.notebookKeyHandler)
            this.boundElement = null
        }
    }

    render() {
        const {notebooks, activeNotebookIdx, notebookCreator} = this.state
        const activeNotebook = (activeNotebookIdx === -1) ? null : notebooks[activeNotebookIdx]
        const {user} = this.props

        const renderNotebook = (element) => {
            if (element) {
                if (!this.panel.isAttached) {
                    element.addEventListener(
                        'keydown',
                        this.notebookKeyHandler,
                        true
                    )
                    this.boundElement = element
                    Widget.attach(this.panel, element)
                }
            }
        }

        return (
            <>
                <div className='notebook-tab-bar'>
                    <Menu className='notebook-tab-items'>
                        {notebooks.map(({id, entity, cluster, local}) =>
                            <NotebookTitle
                                key={id}
                                local={local}
                                tag={id}
                                selected={activeNotebook && activeNotebook.id === id}
                                onClose={(e) => {
                                    e.preventDefault()
                                    e.stopPropagation()
                                    this.closeNotebook(id)
                                }}
                                onSelect={() => this.openNotebook(id, entity, cluster)}
                            />
                        )}
                    </Menu>

                    <ClusterSelector user={user} onSelect={(e, t) => this.newNotebook(e, t)}/>

                </div>
                {activeNotebook ? <NotebookCreator
                    notebookId={activeNotebook.id}
                    open={notebookCreator}
                    onClose={() => this.setState({notebookCreator: false})}
                    onCreate={this.handleNotebookCreate}
                /> : null}
                {activeNotebook ? <NotebookStatus className='notebook-status' widget={activeNotebook.widget}/> : null}
                <div id='notebook' ref={renderNotebook}/>
            </>
        )
    }
}