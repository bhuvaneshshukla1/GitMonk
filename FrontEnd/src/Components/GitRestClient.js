import axios from "axios";

const GIT_CLIENT_URL = "http://127.0.0.1:5000/filterData";

export const execute = async (request) => {
    try {
        console.log(request);
        const response = await axios.post(GIT_CLIENT_URL,request);
        return response.data;
    } catch (error) {
        console.error("Error while connecting to backend",error);
        throw error;
    }
};