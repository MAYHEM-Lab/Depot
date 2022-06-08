import React from 'react';
import {Divider, Grid, Segment} from 'semantic-ui-react';

import './style.css'
import NotebookFrame from "./notebook/notebook_frame";
import {BrowserRouter, Route, Routes} from "react-router-dom";
import {Outlet, UNSAFE_LocationContext as LocationContext, UNSAFE_NavigationContext as NavigationContext} from "react-router";
import Authenticator, {UserContext} from "./auth";
import DatasetHeader from "./dataset";
import DatasetInfo from "./dataset/dataset_info";
import DatasetCode from "./dataset/dataset_code";
import SegmentList from "./dataset/segment_list";
import SegmentInfo from "./dataset/segment_info";
import GithubHandler from "./auth/github_handler";
import EntityProvider from "./entity";
import EntityInfo from "./entity/info";
import NavBar from "./common/navbar";
import Home from "./entity/home";
import Error from "./common/error";
import DatasetManage from "./dataset/manage";

function Content() {
    return <Segment className='app-container'>
        <Authenticator>
            <Grid divided columns={2}>
                <Grid.Column className='dataset-panel'>
                    <NavBar/>
                    <Divider hidden/>
                    <div className='dataset-container'>
                        <Outlet/>
                    </div>
                </Grid.Column>
                <Grid.Column className='notebook-panel'>
                    <div className='notebook-container'>
                        <UserContext.Consumer>
                            {user => (
                                <LocationContext.Consumer>
                                    {location => (
                                        <NavigationContext.Consumer>
                                            {navigator => <NotebookFrame user={user} location={location} navigator={navigator}/>}
                                        </NavigationContext.Consumer>
                                    )}
                                </LocationContext.Consumer>
                            )}
                        </UserContext.Consumer>
                    </div>
                </Grid.Column>
            </Grid>
        </Authenticator>
    </Segment>
}

export default function App() {
    return (
        <BrowserRouter>
            <Routes>
                <Route path='/auth/github' element={<GithubHandler/>}/>
                <Route path='/' element={<Content/>}>
                    <Route path='*' element={<Error/>}/>
                    <Route index element={<Home/>}/>
                    <Route path=':entityName' element={<EntityProvider/>}>
                        <Route index element={<EntityInfo/>}/>
                        <Route path=':datasetTag' element={<DatasetHeader/>}>
                            <Route index element={<DatasetInfo/>}/>
                            <Route path='code' element={<DatasetCode/>}/>
                            <Route path='manage' element={<DatasetManage/>}/>
                            <Route path='segments'>
                                <Route index element={<SegmentList/>}/>
                                <Route path=':version' element={<SegmentInfo/>}/>
                            </Route>
                        </Route>
                    </Route>
                </Route>
            </Routes>
        </BrowserRouter>
    )
}