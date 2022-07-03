import React, {useCallback, useContext, useRef, useState} from 'react';
import {Button, Form, Grid, Header, List, Modal, Segment} from "semantic-ui-react";
import {ValidatingInput} from "../common";
import validateTag from "../common/validate";
import API from "../api";
import {OwnerInput, VisibilityInput} from "../notebook/dataset_creator";
import {UserContext} from "../auth";


function FileUpload({onFiles}) {
    const [dragHighlight, setDragHighlight] = useState(false)
    const [files, setFiles] = useState([])

    const user = useContext(UserContext)

    const uploadFile = async (newFile) => {
        const {id, filename} = await API.startFileUpload(user.name, 1)
        await API.uploadFileChunk(user.name, id, filename, 1, newFile)
        await API.commitFileUpload(user.name, id, filename)
    }

    const addFiles = async (newFiles) => {
        console.log(newFiles)
        await Promise.all(Array.from(newFiles).map(uploadFile))
        const updatedFiles = [...files, ...Array.from(newFiles).map(f => {
            return {file: f, name: f.name}
        })]
        setFiles(updatedFiles)
        onFiles(updatedFiles)
    }

    const dragEvents = useCallback(
        (element) => {
            console.log('drag:', element)
            if (element) {
                const preventDefaults = (e) => {
                    e.preventDefault()
                    e.stopPropagation()
                }

                ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(e => element.addEventListener(e, preventDefaults, false));
                ['dragenter', 'dragover'].forEach(e => element.addEventListener(e, () => setDragHighlight(true), false));
                ['dragleave', 'drop'].forEach(e => element.addEventListener(e, () => setDragHighlight(false), false));

                element.addEventListener('drop', (e) => addFiles(e.dataTransfer.files), false)

            }
        }, [files]
    )

    return <div ref={dragEvents} className={'dataset-upload-file' + (dragHighlight ? ' highlight' : '')}>
        <Button as="label" htmlFor="file" type="button" size='tiny'>Browse...</Button> or drag and drop files here.
        <input type="file" multiple id="file" style={{display: "none"}} onChangeCapture={e => addFiles(e.target.files)}/>
        <List>
            {files.map((file, idx) =>
                <List.Item key={idx}>{file.name}</List.Item>
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

    // const submittable = tag && tagValid
    const submittable = true

    return <Modal
        onMount={() => {
            setTag('')
            setLoading(false)
            setTagValid(false)
            setOwner(null)
            setVisibility('Public')
            setDescription('')
        }}
        dimmer='inverted'
        centered={false}
        size='small'
        open={open}
        onClose={onClose}
    >
        <Modal.Content className='dataset-upload-menu'>
            <Header>Upload Unmanaged Dataset</Header>
            <Form onSubmit={async () => {
                setLoading(true)
                try {
                    // API.uploadDataset("t1", "t2", files)
                    // await onCreate(tag)
                } finally {
                    setLoading(false)
                }
            }}>
                <Grid padded columns={2} className='dataset-upload-info'>
                    <Grid.Column>
                        <Segment basic>
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
                            <Form.TextArea label='Description' onChange={(e, d) => setDescription(d.value)}/>
                        </Segment>
                    </Grid.Column>
                    <Grid.Column stretched>
                        <FileUpload onFiles={setFiles}/>
                    </Grid.Column>
                </Grid>
                <Form.Button disabled={!submittable} primary loading={loading} type='submit'>Create</Form.Button>
            </Form>
        </Modal.Content>
    </Modal>
}


