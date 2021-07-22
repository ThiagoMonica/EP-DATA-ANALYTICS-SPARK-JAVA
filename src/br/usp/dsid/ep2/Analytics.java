package br.usp.dsid.ep2;

import java.util.List;

public class Analytics {

	
	public Double calculaSoma(List<Double> dados) {
		Double soma = 0D;
		
		for(int i=0; i < dados.size(); i++) {
			soma += dados.get(i);
		}
		
		return soma;
	}
	
	public Double calculaMedia(List<Double> dados) {
		Double soma = calculaSoma(dados);
		return soma/dados.size();
	}
	
	public Double calculaDesvioPadrao(List<Double> dados) {
		if(dados.size() <= 1) {
			System.out.println("Tamanho da amostra menor ou igual a 1");
			return 0D;
		}

		Double media = calculaMedia(dados);
		Double aux = 0D;
		
		for(Double valor : dados) {
			aux += Math.pow(valor - media, 2);
		}
		
		return Math.sqrt(aux/dados.size()-1);
	}
}
