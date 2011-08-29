package org.riedelcastro.cmonnoun.clusterhub.nlp

/**
 * @author sriedel
 */
case class Token(word: String, charOffsetBegin: Int, charOffsetEnd: Int)
case class Sentence(txt:String, charOffsetBegin: Int, charOffsetEnd: Int)