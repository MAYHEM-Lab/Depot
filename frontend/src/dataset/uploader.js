import React, {useContext, useEffect, useState} from 'react';
import {Button, Divider, Form, Grid, Header, Icon, Input, List, Modal, Segment} from 'semantic-ui-react';
import {ValidatingInput} from '../common';
import validateTag from '../common/validate';
import API from '../api';
import {OwnerInput, VisibilityInput} from './creator';
import {UserContext} from '../auth';
import util from '../util';

function Semaphore(max, fn, ...a1) {
    let run = 0;
    const waits = [];

    function next(x) {
        if (run < max && waits.length)
            waits.shift()(++run);
        return x;
    }

    return (...a2) => next(new Promise(ok => waits.push(ok)).then(() => fn(...a1, ...a2)).finally(_ => run--).finally(next));
}

export function FileUpload({onFiles}) {
    const [dragHighlight, setDragHighlight] = useState(0)
    const [files, setFiles] = useState([])
    const [curFileId, setFileId] = useState(0)

    const user = useContext(UserContext)

    useEffect(() => onFiles(files), [files])

    const updateFile = (fileId, state) => {
        setFiles(files => {
            const curFileIdx = files.findIndex(f => f.id === fileId)
            if (curFileIdx !== -1) {
                const head = files.slice(0, curFileIdx)
                const tail = files.slice(curFileIdx + 1)
                const n = state ?
                    [...head, Object.assign({}, files[curFileIdx], state), ...tail] :
                    [...head, ...tail]
                return n
            }
            return files
        })
    }

    const removeFile = async (fileId) => {
        const curFile = files.find(f => f.id === fileId)
        if (curFile) {
            const {uploadId, filename, controller} = curFile
            if (controller) {
                controller.abort()
            }
            if (uploadId && filename) {
                try {
                    await API.deleteFileUpload(user.name, uploadId, filename)
                } catch {
                }
            }
        }
        updateFile(fileId, null)
    }

    const uploadChunk = (signal, fileId, url, file) => new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.onload = e => {
            if (xhr.status >= 200 && xhr.status < 300) {
                resolve(xhr.response)
            } else {
                reject({status: xhr.status, statusText: xhr.statusText})
            }
        }
        xhr.onerror = () => {
            reject({status: xhr.status, statusText: xhr.statusText})
        }
        xhr.upload.onprogress = ({loaded}) => {
            updateFile(fileId, {loaded: loaded})
        }
        xhr.open('PUT', url)
        xhr.send(file)
        signal.addEventListener('abort', () => {
            xhr.abort()
            reject({status: -1, statusText: 'Aborted'})
        })
    })

    const uploadFile = async (fileId, file) => {
        try {
            const controller = new AbortController()
            const {id, filename, parts} = await API.startFileUpload(user.name, 1, file.type || null)
            updateFile(fileId, {uploadId: id, filename: filename, state: 'uploading', controller: controller})

            await uploadChunk(controller.signal, fileId, parts[0], file)

            // await API.uploadFileChunk(user.name, id, filename, 1, file, controller.signal, onFileUploadProgress, onFilePersistProgress)
            await API.commitFileUpload(user.name, id, filename)
            updateFile(fileId, {state: 'success'})
        } catch {
            updateFile(fileId, {state: 'error'})
        }
    }

    const addFiles = (newFiles) => {
        const updatedFiles = [...files, ...Array.from(newFiles).map((f, idx) => {
            const fileId = curFileId + idx
            uploadFile(fileId, f)
            return {
                file: f,
                name: f.name,
                id: fileId,
                state: 'pending',
                size: f.size,
                loaded: 0
            }
        })]
        setFileId(curFileId + newFiles.length)
        setFiles(updatedFiles)
    }

    const preventDefaults = (e) => {
        e.preventDefault()
        e.stopPropagation()
    }

    const duplicates = files.map(({name, id}) =>
        !!files.find(f => f.name.toLowerCase() === name.toLowerCase() && f.id !== id)
    )

    return <div
        onDragOver={preventDefaults}
        onDragEnter={e => {
            preventDefaults(e)
            setDragHighlight(d => d + 1)
        }}
        onDragLeave={e => {
            preventDefaults(e)
            setDragHighlight(d => d - 1)
        }}
        onDrop={e => {
            preventDefaults(e)
            setDragHighlight(0)
            addFiles(e.dataTransfer.files)
        }}
        className={'dataset-upload' + (dragHighlight ? ' highlight' : '')}
    >
        <Segment basic textAlign='center' className='dataset-upload-target'>
            <Segment basic>
                <Icon size='huge' name='upload' color={dragHighlight ? 'yellow' : 'black'}/>
            </Segment>
            <span>Drag and drop files here</span>
            <Divider horizontal>Or</Divider>
            <Button as='label' htmlFor='file' type='button' size='small'>Browse...</Button>
            <input value='' readOnly type='file' multiple id='file' style={{display: 'none'}} onChangeCapture={e => addFiles(e.target.files)}/>
        </Segment>
        <List divided className='dataset-upload-file-list'>
            {files.map(({file, name, state, size, id, loaded}, idx) => {
                    const done = state === 'success'
                    return <List.Item key={idx}>
                        <Grid columns={3} className='dataset-upload-file'>
                            <Grid.Row className='dataset-upload-file-row-info'>
                                <Grid.Column textAlign='left' width={10}>
                                    <Input
                                        error={duplicates[idx]}
                                        autoFocus
                                        fluid
                                        defaultValue={file.name}
                                        onChange={e => updateFile(id, {name: e.target.value})}
                                        className='dataset-upload-file-row-input'
                                    />
                                    <pre title={file.name} className='dataset-upload-file-localname'>{file.name}</pre>
                                </Grid.Column>
                                <Grid.Column verticalAlign='middle' textAlign='right' width={5}>
                                    {
                                        done ?
                                            <div className='dataset-upload-file-size'>
                                                {util.formatBytes(size)}
                                            </div> :
                                            <div className='dataset-upload-file-progress' data-label={util.formatBytes(loaded) + ' of ' + util.formatBytes(size)}>
                                                <span className='value' style={{width: `${Math.round(100 * loaded / size)}%`}}/>
                                            </div>
                                    }
                                </Grid.Column>
                                <Grid.Column verticalAlign='middle' width={1}>
                                    <span className='dataset-upload-file-remove' onClick={() => removeFile(id)}>✖</span>
                                </Grid.Column>
                            </Grid.Row>
                        </Grid>
                    </List.Item>
                }
            )}
        </List>
    </div>
}

export default function DatasetUploader({open, onClose, onCreate}) {
    const [tag, setTag] = useState('')
    const [loading, setLoading] = useState(false)
    const [tagValid, setTagValid] = useState(false)
    const [owner, setOwner] = useState(null)
    const [visibility, setVisibility] = useState('Public')
    const [description, setDescription] = useState('')
    const [files, setFiles] = useState([])

    const [step, setStep] = useState(0)

    const uploadable = files.length &&
        !files.find(f => f.state !== 'success') &&
        new Set(files.map(f => f.name)).size === files.length

    const creatable = tag && tagValid && owner && uploadable

    const hidden = {display: 'none'}

    return <Modal
        onUnmount={() => {
            setTag('')
            setLoading(false)
            setTagValid(false)
            setOwner(null)
            setVisibility('Public')
            setDescription('')
            setFiles([])
            setStep(0)
        }}
        dimmer='inverted'
        centered={false}
        size={step ? 'small' : 'large'}
        open={open}
        onClose={onClose}
    >
        <Modal.Content className='dataset-upload-menu'>
            <Header>Upload Unmanaged Dataset</Header>
            <div style={step === 0 ? null : hidden}>
                <FileUpload onFiles={setFiles}/>
                <Button disabled={!uploadable} primary onClick={() => setStep(1)}>Next</Button>
            </div>
            <Form style={step === 1 ? null : hidden} onSubmit={async () => {
                setLoading(true)
                try {
                    await onCreate(owner, tag, description, {type: 'Raw'}, visibility, files.reduce((o, f) => {
                        o[f.name] = f.filename
                        return o
                    }, {}))
                } finally {
                    setLoading(false)
                }
            }}>
                <Segment basic className='dataset-upload-info'>
                    <ValidatingInput
                        required
                        label='Name'
                        placeholder='ID'
                        onValidate={setTagValid}
                        onInput={setTag}
                        sync={validateTag}
                        async={async (tag) => {
                            const validate = await API.validateDatasetTag(tag)
                            if (!validate.valid) return 'A dataset with this ID already exists'
                            return null
                        }}
                    />
                    <OwnerInput onSelect={setOwner}/>
                    <VisibilityInput onSelect={setVisibility}/>
                    <Form.TextArea rows={15} label='Description' onChange={(e, d) => setDescription(d.value)}/>
                </Segment>
                <div>
                    <Button secondary onClick={() => setStep(0)} type='button'>Back</Button>
                    <Button disabled={!creatable} primary loading={loading} type='submit'>Create</Button>
                </div>
            </Form>
        </Modal.Content>
    </Modal>
}
