from langchain import text_splitter
from langchain_community.vectorstores import FAISS
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough

data = """This is a long text that needs to be split into smaller chunks for processing. 
The text splitter will ensure that the chunks are of manageable size and overlap to maintain context. 
This is especially useful for tasks like question answering or summarization where the model 
needs to understand the context of the text. By splitting the text into smaller pieces, 
we can feed it into a language model without running into token limits.
"""

text_splitter = text_splitter.RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
docs = text_splitter.create_documents(list(data))

embeddings = None
vectorstore = FAISS.from_documents(docs, embeddings)

retriever = vectorstore.as_retriever()

template = """
Ты - ассистент, отвечающий на вопросы. 
Используй только предоставленный контекст для ответа.
Контекст: {context}
Вопрос: {question}
Ответ:
"""
prompt = ChatPromptTemplate.from_template(template)

rag_chain = (
    {"context": retriever, "question": RunnablePassthrough()}
    | prompt
    | StrOutputParser()
)

query = "Для чего используется LangSmith в LangChain?"
response = rag_chain.invoke(query)
print(response)
