import React, {useState} from "react";
import { useNavigate } from 'react-router-dom';
import 'bootstrap/dist/css/bootstrap.min.css';
import './Login.css';
import axios from "axios";

const users = [
    {
        username: "bhuvaneshshukla1",
        password: "1234"
    }
];

const GIT_CLIENT_URL = import.meta.env.VITE_GIT_CLIENT_URL || "http://127.0.0.1:5000";
console.log(GIT_CLIENT_URL)
function Login() {
    const[username, setUsername] = useState("");
    const[password,setPassword] = useState("");
    const[error, setError] = useState("");
    const [message, setMessage] = useState("");
    const [isSignUp, setIsSignUp] = useState(false);
    const navigate = useNavigate();

    const handleSignUp = async (e) => {
        e.preventDefault();
        setError("")
        try {
            const response = await axios.post(GIT_CLIENT_URL + '/createUser', { "username":username, "password":password });
            if (response.status === 200) {
                if(response.data.result === "user already exists"){
                    setMessage("Already Signed Up!");
                }
                else{
                    setMessage("Sign Up successful!");
                }

                setIsSignUp(true);
            }
        } catch (error) {
            setError("Sign Up failed. Please try again.");
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError("")
        try {
            const response = await axios.post(GIT_CLIENT_URL + '/validUser', { "username":username, "password":password });
            if (response.status === 200 && response.data && response.data.result === 'success') {
                navigate("/app", {state: {username}});
            }
            else {
                setError("Invalid Credentials");
            }
        } catch (error) {
            setError("Sign In failed. Please try again.");
        }

    };

    return (
        <div className="d-flex justify-content-center align-items-center vh-100">
            <div className="card p-4" style={{ width: '400px' }}>
            <h1 className="text-center my-4">Login</h1>
            <form onSubmit={handleSubmit} className="mb-4">
                    <div className="form-group mb-3">
                    <label htmlFor="username">Username</label>
                    <input type="text" className="form-control" id="username" value={username}
                           onChange={(e) => setUsername(e.target.value)}/>

                </div>
                    <div className="form-group mb-3">
                    <label htmlFor="password">Password</label>
                        <input
                            type="password"
                            className="form-control"
                            id="password"
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                        />
                </div>
                    {error && <div className="alert alert-danger">{error}</div>}
                    {message && <div className="alert alert-success">{message}</div>}
                    <button type="submit" className="btn btn-primary w-100">Login</button>
            </form>
                {!isSignUp && (
                    <button onClick={handleSignUp} className="btn btn-secondary w-100">Sign Up</button>
                )}
        </div>
        </div>
    );
}

export default Login;