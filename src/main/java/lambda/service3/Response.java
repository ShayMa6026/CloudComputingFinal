/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda.service3;

import java.util.Map;

public class Response extends saaf.Response {

    Map<String, Object> result;

    public Map<String, Object> getResult() {
        return result;
    }

    public void setResult(Map<String, Object> result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "Response{" +
                "result=" + result +
                '}';
    }
}
