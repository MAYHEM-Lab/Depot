import React from 'react';
import {Divider, Grid} from 'semantic-ui-react';

import './style.css'
import NotebookFrame from "./notebook/notebook_frame";
import {BrowserRouter, Route, Routes} from "react-router-dom";
import {Outlet, UNSAFE_LocationContext as LocationContext, UNSAFE_NavigationContext as NavigationContext} from "react-router";
import Authenticator, {UserContext} from "./auth";
import DatasetHeader from "./dataset";
import DatasetInfo from "./dataset/info";
import DatasetCode from "./dataset/code";
import SegmentList from "./dataset/segment/list";
import SegmentInfo from "./dataset/segment/info";
import GithubHandler from "./auth/github_handler";
import EntityProvider from "./entity";
import EntityInfo from "./entity/info";
import NavBar from "./common/navbar";
import Home from "./home";
import Error from "./common/error";
import DatasetManage from "./dataset/manage";
import {EventContext} from "./common/bus";
import NotebookCode from "./notebook/view";

function Content() {
    return <div className='app-container'>
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
                        <EventContext.Consumer>
                            {eventBus => (
                                <UserContext.Consumer>
                                    {user => (
                                        <LocationContext.Consumer>
                                            {location => (
                                                <NavigationContext.Consumer>
                                                    {navigator =>
                                                        user ?
                                                            <NotebookFrame user={user} location={location} navigator={navigator} eventBus={eventBus}/>
                                                            : <div>Please log in</div>
                                                    }
                                                </NavigationContext.Consumer>
                                            )}
                                        </LocationContext.Consumer>
                                    )}
                                </UserContext.Consumer>
                            )}
                        </EventContext.Consumer>
                    </div>
                </Grid.Column>
            </Grid>
        </Authenticator>
    </div>
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
                        <Route path='notebooks/:notebookTag' element={<NotebookCode/>}/>
                        <Route path='datasets/:datasetTag' element={<DatasetHeader/>}>
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