// Filters.jsx
import React, { useState } from "react";
import 'bootstrap/dist/css/bootstrap.min.css';
import './Filters.css';

function Filters({ onSubmit }) {
    const [repository, setRepository] = useState("");
    const [project, setProject] = useState("");
    const [fromDate, setFromDate] = useState("");
    const [author, setAuthor] = useState(false);

    const handleSubmit = (e) => {
        e.preventDefault();
        onSubmit({
            repository,
            project,
            fromDate,
            author
        });
    };

    return (
        <div className="card shadow-sm p-3 mb-5 bg-white rounded">
            <div className="card-body">
                <form onSubmit={handleSubmit}>
                    <div className="d-flex justify-content-around align-items-center flex-wrap">
                            <div className="input-box">
                                <label htmlFor="repository">Repository</label>
                                <input type="text" className="form-control" id="repository" value={repository}
                                       onChange={(e) => setRepository(e.target.value)}/>
                            </div>
                            <div className="input-box">
                                <label htmlFor="project">Project</label>
                                <input type="text" className="form-control" id="project" value={project}
                                       onChange={(e) => setProject(e.target.value)}/>
                            </div>
                            <div className="input-box">
                                <label htmlFor="fromDate">From</label>
                                <input type="datetime-local" className="form-control" id="fromDate" value={fromDate}
                                       onChange={(e) => setFromDate(e.target.value)}/>
                            </div>
                        <div className="input-box checkbox">
                            <input type="checkbox" className="form-check-input" id="author" checked={author}
                                       onChange={(e) => setAuthor(e.target.checked)}/>
                                <label className="form-check-label" htmlFor="author">You as author</label>
                            </div>
                        <div>
                            <button type="submit" className="btn btn-primary mt-2">Filter</button>
                        </div>
                    </div>
                </form>
            </div>
        </div>
    );
}

export default Filters;
